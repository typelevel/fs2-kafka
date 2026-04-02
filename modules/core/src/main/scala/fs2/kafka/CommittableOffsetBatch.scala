/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Foldable, Show}
import cats.instances.list.*
import cats.syntax.foldable.*
import cats.syntax.show.*
import cats.ApplicativeError
import fs2.kafka.instances.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * [[CommittableOffsetBatch]] represents a batch of Kafka [[offsets]] which can be committed
  * together using [[commit]]. An offset, or one more batch, can be added an existing batch using
  * `updated`. Note that this requires the offsets per topic-partition to be included in-order,
  * since offset commits in general require it.<br><br>
  *
  * Use [[CommittableOffsetBatch#empty]] to create an empty batch. The [[CommittableOffset#batch]]
  * function can be used to create a batch from an existing [[CommittableOffset]].<br><br>
  *
  * If you have some offsets in-order per topic-partition, you can fold them together using
  * [[CommittableOffsetBatch#empty]] and `updated`, or you can use
  * [[CommittableOffsetBatch#fromFoldable]]. Generally, prefer to use `fromFoldable`, as it has
  * better performance. Provided pipes like [[commitBatchWithin]] are also to be preferred, as they
  * also achieve better performance.
  */
sealed abstract class CommittableOffsetBatch[F[_]] {

  /**
    * Creates a new [[CommittableOffsetBatch]] with the specified offset included. Note that this
    * function requires offsets to be in-order per topic-partition, as provided offsets will
    * override existing offsets for the same topic-partition.
    */
  def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F]

  /**
    * Creates a new [[CommittableOffsetBatch]] with the specified offsets included. Note that this
    * function requires offsets to be in-order per topic-partition, as provided offsets will
    * override existing offsets for the same topic-partition.
    */
  def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F]

  /**
    * The offsets included in the [[CommittableOffsetBatch]].
    */
  def offsets: Map[TopicPartition, OffsetAndMetadata]

  /**
    * The consumer group IDs for the [[offsets]] in the batch. For the batch to be valid and for
    * [[commit]] to succeed, there should be exactly one ID in the set and the flag
    * [[consumerGroupIdsMissing]] should be `false`.<br><br>
    *
    * There might be more than one consumer group ID in the set if offsets from multiple consumers,
    * with different group IDs, have accidentally been mixed. The set might also be empty if no
    * consumer group IDs have been specified.
    */
  def consumerGroupIds: Set[String]

  /**
    * `true` if any offset in the batch came from a consumer without a group ID; `false` otherwise.
    * For the batch to be valid and for [[commit]] to succeed, this flag must be `false` and there
    * should be exactly one consumer group ID in [[consumerGroupIds]].
    */
  def consumerGroupIdsMissing: Boolean

  /**
    * Commits the [[offsets]] to Kafka in a single commit. For the batch to be valid and for commit
    * to succeed, the following conditions must hold:<br>
    *   - [[consumerGroupIdsMissing]] must be false, and<br>
    *   - [[consumerGroupIds]] must have exactly one ID.<br><br>
    *
    * If one of the conditions above do not hold, there will be a [[ConsumerGroupException]]
    * exception raised and a commit will not be attempted. If [[offsets]] is empty then these
    * conditions do not need to hold, as there is nothing to commit.
    */
  def commit: F[Unit]

}

object CommittableOffsetBatch {

  private[kafka] def apply[F[_]](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    consumerGroupIds: Set[String],
    consumerGroupIdsMissing: Boolean,
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  )(implicit F: ApplicativeError[F, Throwable]): CommittableOffsetBatch[F] = {
    val _offsets                 = offsets
    val _consumerGroupIds        = consumerGroupIds
    val _consumerGroupIdsMissing = consumerGroupIdsMissing
    val _commit                  = commit

    new CommittableOffsetBatch[F] {

      override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] =
        CommittableOffsetBatch(
          _offsets.updated(that.topicPartition, that.offsetAndMetadata),
          that.consumerGroupId.fold(_consumerGroupIds)(_consumerGroupIds + _),
          _consumerGroupIdsMissing || that.consumerGroupId.isEmpty,
          _commit
        )

      override def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
        CommittableOffsetBatch(
          _offsets ++ that.offsets,
          _consumerGroupIds ++ that.consumerGroupIds,
          _consumerGroupIdsMissing || that.consumerGroupIdsMissing,
          _commit
        )

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        _offsets

      override val consumerGroupIds: Set[String] =
        _consumerGroupIds

      override val consumerGroupIdsMissing: Boolean =
        _consumerGroupIdsMissing

      override def commit: F[Unit] =
        if (_consumerGroupIdsMissing || _consumerGroupIds.size != 1)
          F.raiseError(ConsumerGroupException(consumerGroupIds))
        else _commit(offsets)

      override def toString: String =
        Show[CommittableOffsetBatch[F]].show(this)

    }
  }

  /**
    * Creates a [[CommittableOffsetBatch]] from some [[CommittableOffset]]s, where the containing
    * type has a `Foldable` instance. Guaranteed to be equivalent to the following, but implemented
    * more efficiently.
    *
    * {{{
    * offsets.foldLeft(CommittableOffsetBatch.empty[F])(_ updated _)
    * }}}
    *
    * Note that just like for `updated`, `offsets` have to be in order per topic-partition.
    *
    * @see
    *   [[CommittableOffsetBatch#fromFoldableMap]]
    * @see
    *   [[CommittableOffsetBatch#fromFoldableOption]]
    */
  def fromFoldable[F[_], G[_]](offsets: G[CommittableOffset[F]])(implicit
    F: ApplicativeError[F, Throwable],
    G: Foldable[G]
  ): CommittableOffsetBatch[F] =
    fromFoldableMap(offsets)(identity)

  /**
    * Creates a [[CommittableOffsetBatch]] from a `Foldable` containing `A`s, by applying `f` to
    * each `A` to get the [[CommittableOffset]]. Guaranteed to be equivalent to the following, but
    * implemented more efficiently.
    *
    * {{{
    * ga.foldLeft(CommittableOffsetBatch.empty[F])(_ updated f(_))
    * }}}
    *
    * Note that just like for `updated`, `offsets` have to be in order per topic-partition.
    *
    * @see
    *   [[CommittableOffsetBatch#fromFoldable]]
    * @see
    *   [[CommittableOffsetBatch#fromFoldableOption]]
    */
  def fromFoldableMap[F[_], G[_], A](ga: G[A])(f: A => CommittableOffset[F])(implicit
    F: ApplicativeError[F, Throwable],
    G: Foldable[G]
  ): CommittableOffsetBatch[F] = {
    var commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] = null
    var offsetsMap: Map[TopicPartition, OffsetAndMetadata]        = Map.empty
    var consumerGroupIds: Set[String]                             = Set.empty
    var consumerGroupIdsMissing: Boolean                          = false
    var empty: Boolean                                            = true

    ga.foldLeft(()) { (_, a) =>
      val offset = f(a)

      if (empty) {
        commit = offset.commitOffsets
        empty = false
      }

      offsetsMap = offsetsMap.updated(offset.topicPartition, offset.offsetAndMetadata)
      offset.consumerGroupId match {
        case Some(consumerGroupId) => consumerGroupIds = consumerGroupIds + consumerGroupId
        case None                  => consumerGroupIdsMissing = true
      }
    }

    if (empty) CommittableOffsetBatch.empty[F]
    else CommittableOffsetBatch(offsetsMap, consumerGroupIds, consumerGroupIdsMissing, commit)
  }

  /**
    * Creates a [[CommittableOffsetBatch]] from some [[CommittableOffset]]s wrapped in `Option`,
    * where the containing type has a `Foldable` instance. Guaranteed to be equivalent to the
    * following, but implemented more efficiently.
    *
    * {{{
    * offsets.foldLeft(CommittableOffsetBatch.empty[F]) {
    *   case (batch, Some(offset)) => batch.updated(offset)
    *   case (batch, None)         => batch
    * }
    * }}}
    *
    * Note that just like for `updated`, `offsets` have to be in order per topic-partition.
    *
    * @see
    *   [[CommittableOffsetBatch#fromFoldable]]
    * @see
    *   [[CommittableOffsetBatch#fromFoldableMap]]
    */
  def fromFoldableOption[F[_], G[_]](offsets: G[Option[CommittableOffset[F]]])(implicit
    F: ApplicativeError[F, Throwable],
    G: Foldable[G]
  ): CommittableOffsetBatch[F] = {
    var commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] = null
    var offsetsMap: Map[TopicPartition, OffsetAndMetadata]        = Map.empty
    var consumerGroupIds: Set[String]                             = Set.empty
    var consumerGroupIdsMissing: Boolean                          = false
    var empty: Boolean                                            = true

    offsets.foldLeft(()) {
      case (_, Some(offset)) =>
        if (empty) {
          commit = offset.commitOffsets
          empty = false
        }

        offsetsMap = offsetsMap.updated(offset.topicPartition, offset.offsetAndMetadata)
        offset.consumerGroupId match {
          case Some(consumerGroupId) => consumerGroupIds = consumerGroupIds + consumerGroupId
          case None                  => consumerGroupIdsMissing = true
        }
      case (_, None) => ()
    }

    if (empty) CommittableOffsetBatch.empty[F]
    else CommittableOffsetBatch(offsetsMap, consumerGroupIds, consumerGroupIdsMissing, commit)
  }

  /**
    * An empty [[CommittableOffsetBatch]] which does not include any offsets and `commit` will not
    * commit offsets. This can be used together with `updated` to create a batch from some offsets.
    *
    * @see
    *   [[CommittableOffsetBatch#fromFoldable]]
    * @see
    *   [[CommittableOffsetBatch#fromFoldableOption]]
    */
  def empty[F[_]](implicit F: Applicative[F]): CommittableOffsetBatch[F] =
    new CommittableOffsetBatch[F] {

      override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] =
        that.batch

      override def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
        that

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        Map.empty

      override val consumerGroupIds: Set[String] =
        Set.empty

      override val consumerGroupIdsMissing: Boolean =
        false

      override val commit: F[Unit] =
        F.unit

      override def toString: String =
        Show[CommittableOffsetBatch[F]].show(this)

    }

  implicit def committableOffsetBatchShow[F[_]]: Show[CommittableOffsetBatch[F]] =
    Show.show { cob =>
      if (cob.offsets.isEmpty) "CommittableOffsetBatch(<empty>)"
      else {
        cob
          .offsets
          .toList
          .sorted
          .mkStringAppend { case (append, (tp, oam)) =>
            append(tp.show)
            append(" -> ")
            append(oam.show)
          }(
            start = "CommittableOffsetBatch(",
            sep = ", ",
            end = ")"
          )
      }
    }

}
