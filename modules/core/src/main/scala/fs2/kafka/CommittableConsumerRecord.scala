/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Bitraverse, Eq, Eval, Show, Traverse}
import cats.syntax.bifoldable.*
import cats.syntax.bitraverse.*
import cats.syntax.eq.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*

/**
  * [[CommittableConsumerRecord]] is a Kafka record along with an instance of [[CommittableOffset]],
  * which can be used commit the record offset to Kafka. Offsets are normally committed in batches,
  * either using [[CommittableOffsetBatch]] or via pipes, like [[commitBatchWithin]]. If you are not
  * committing offsets to Kafka then you can use [[record]] to get the underlying record and also
  * discard the [[offset]].<br><br>
  *
  * While normally not necessary, [[CommittableConsumerRecord#apply]] can be used to create a new
  * instance.
  */
sealed abstract class CommittableConsumerRecord[F[_], +K, +V] {

  /**
    * The Kafka record for the [[CommittableConsumerRecord]]. If you are not committing offsets to
    * Kafka, simply use this to get the [[ConsumerRecord]] and discard the [[offset]].
    */
  def record: ConsumerRecord[K, V]

  /**
    * A [[CommittableOffset]] instance, providing a way to commit the [[record]] offset to Kafka.
    * This is normally done in batches as it achieves better performance. Pipes like
    * [[commitBatchWithin]] use [[CommittableOffsetBatch]] to batch and commit offsets.
    */
  def offset: CommittableOffset[F]

}

object CommittableConsumerRecord {

  final private[this] class CommittableConsumerRecordImpl[F[_], +K, +V](
    override val record: ConsumerRecord[K, V],
    override val offset: CommittableOffset[F]
  ) extends CommittableConsumerRecord[F, K, V] {

    override def toString: String =
      s"CommittableConsumerRecord($record, $offset)"

  }

  /**
    * Creates a new [[CommittableConsumerRecord]] using the specified Kafka [[ConsumerRecord]] and
    * [[CommittableOffset]], which can be used to commit the record offset to Kafka.
    */
  def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    offset: CommittableOffset[F]
  ): CommittableConsumerRecord[F, K, V] =
    new CommittableConsumerRecordImpl(record, offset)

  def unapply[F[_], K, V](
    committable: CommittableConsumerRecord[F, K, V]
  ): Some[(ConsumerRecord[K, V], CommittableOffset[F])] =
    Some((committable.record, committable.offset))

  implicit def committableConsumerRecordShow[F[_], K: Show, V: Show]
    : Show[CommittableConsumerRecord[F, K, V]] = Show.show { cm =>
    show"CommittableConsumerRecord(${cm.record}, ${cm.offset})"
  }

  implicit def committableConsumerRecordEq[F[_], K: Eq, V: Eq]
    : Eq[CommittableConsumerRecord[F, K, V]] =
    Eq.instance { case (l, r) =>
      l.record === r.record && l.offset === r.offset
    }

  implicit def committableConsumerRecordBitraverse[F[_]]
    : Bitraverse[CommittableConsumerRecord[F, *, *]] =
    new Bitraverse[CommittableConsumerRecord[F, *, *]] {

      override def bitraverse[G[_], A, B, C, D](
        fab: CommittableConsumerRecord[F, A, B]
      )(f: A => G[C], g: B => G[D])(implicit
        G: Applicative[G]
      ): G[CommittableConsumerRecord[F, C, D]] =
        fab
          .record
          .bitraverse(f, g)
          .map { (cd: ConsumerRecord[C, D]) =>
            CommittableConsumerRecord(cd, fab.offset)
          }

      override def bifoldLeft[A, B, C](
        fab: CommittableConsumerRecord[F, A, B],
        c: C
      )(f: (C, A) => C, g: (C, B) => C): C =
        fab.record.bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](
        fab: CommittableConsumerRecord[F, A, B],
        c: Eval[C]
      )(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        fab.record.bifoldRight(c)(f, g)

    }

  implicit def committableConsumerRecordTraverse[F[_], K]
    : Traverse[CommittableConsumerRecord[F, K, *]] =
    new Traverse[CommittableConsumerRecord[F, K, *]] {

      override def traverse[G[_], A, B](
        fa: CommittableConsumerRecord[F, K, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[CommittableConsumerRecord[F, K, B]] =
        fa.record
          .traverse(f)
          .map { (b: ConsumerRecord[K, B]) =>
            CommittableConsumerRecord(b, fa.offset)
          }

      override def foldLeft[A, B](fa: CommittableConsumerRecord[F, K, A], b: B)(
        f: (B, A) => B
      ): B =
        fa.record.foldLeft(b)(f)

      override def foldRight[A, B](
        fa: CommittableConsumerRecord[F, K, A],
        lb: Eval[B]
      )(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa.record.foldRight(lb)(f)

    }

}
