/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.KafkaByteShareConsumer
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.CommitCallback.CallbackResults
import fs2.kafka.internal.ShareConsumerCore.*
import fs2.kafka.internal.ShareConsumerState.Acks
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.TimeoutException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
  * The Java share consumer and the records it holds: the consumer's only access to both.<br><br>
  *
  * Acknowledgements are taken from the state and given to Java in one `step`, under the mutex and
  * uncancelable, so that nothing is lost in between. Operations which only change the state run the
  * `ShareConsumerState` method of the same name.
  *
  * @param results
  *   what the commit callback reported since it was last read or cleared
  */
final private[kafka] class ShareConsumerCore[F[_]] private (
  withConsumer: WithShareConsumer[F],
  mutex: Mutex[F],
  state: SignallingRef[F, ShareConsumerState],
  results: Ref[F, CallbackResults]
)(implicit F: Async[F])
  extends ScheduledCore[F] {

  def metrics: F[Map[MetricName, Metric]] =
    withConsumer.blocking(_.metrics().asScala.toMap)

  def subscribe(topics: List[String]): F[Unit] =
    withConsumer.blocking(_.subscribe(topics.asJava))

  def subscription: F[SortedSet[String]] =
    withConsumer.blocking(_.subscription().toSortedSet)

  override def subscribed: F[Boolean] =
    withConsumer.blocking(!_.subscription().isEmpty)

  /**
    * For implicit mode, where a commit would accept records still being processed.
    */
  def unsubscribe: F[Unit] =
    withConsumer.blocking(_.unsubscribe())

  def commit: F[Unit] =
    withConsumer.blocking(_.commitAsync())

  override def held: Signal[F, HeldRecords] = state.map(s => s: HeldRecords)

  def renewing: F[Boolean] = state.get.map(_.renewing)

  def awaitRenewals: F[Unit] = state.waitUntil(!_.renewing)

  def hold(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    start: FiniteDuration,
    reportedLock: Option[FiniteDuration]
  ): F[Unit] =
    state.update(_.hold(deliveries, start, reportedLock))

  def close(kind: AcknowledgeType): F[Unit] =
    F.monotonic.flatMap(at => state.update(_.close(kind, at)))

  def acknowledge(acks: Chunk[Acknowledgement]): F[Vector[TopicPartitionOffset]] =
    F.monotonic.flatMap(at => state.modify(_.acknowledge(acks, at)))

  override def skipRenewal(at: FiniteDuration): F[Unit] =
    state.update(_.skipRenewal(at))

  override def failRenewal: F[Unit] =
    F.monotonic.flatMap(at => state.update(_.failRenewal(at)))

  /**
    * Polls, which sends the pending acknowledgements. Returns `None` if unsubscribed, as Java's
    * poll would throw. The callback's reports are dropped, as no renewal is pending.
    */
  def poll(timeout: FiniteDuration): F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
    results.set(Vector.empty) *> sendingUnsent { consumer =>
      if (consumer.subscription().isEmpty) None else Some(pollRecords(consumer, timeout))
    }

  /**
    * Returns the first error the broker returned, unless unsubscribed, as the records went with the
    * subscription.
    */
  def commitSync(timeout: FiniteDuration): F[Option[KafkaException]] =
    sendingUnsent { consumer =>
      val committed = consumer.commitSync(timeout.toJava)
      if (consumer.subscription().isEmpty) None
      else committed.values.asScala.flatMap(_.toScala).headOption
    }

  /**
    * Unsubscribes also if the commit fails, and then raises the commit's error first. A commit
    * which times out is not raised.
    */
  def commitAndUnsubscribe(timeout: FiniteDuration): F[Unit] =
    step(_.takeUnsent) { (consumer, acks) =>
      val committed = Either.catchNonFatal {
        recordIn(consumer, acks)
        Either.catchOnly[TimeoutException](consumer.commitSync(timeout.toJava)).void.getOrElse(())
      }
      val unsubscribed = Either.catchNonFatal(consumer.unsubscribe())
      (committed, unsubscribed) match {
        case (Left(error), Left(other)) if other ne error => error.addSuppressed(other)
        case _ => ()
      }
      (committed *> unsubscribed).fold(error => throw error, identity)
    }

  override def send: F[Unit] =
    sending(_.takeUnsent)

  /**
    * Acknowledges the records held for `max` without an acknowledgement with `kind`, and sends
    * them.
    */
  override def sendOverdue(max: FiniteDuration, kind: AcknowledgeType): F[Unit] =
    F.monotonic.flatMap(now => sending(_.takeOverdue(max, kind, now)))

  /**
    * Sends the pending acknowledgements and a `Renew` for every unacknowledged record, if the
    * renewal is still `due`, without waiting. Returns the records Java took, and the lock last
    * reported.
    */
  override def renew(at: FiniteDuration)(
    due: HeldRecords => Boolean
  ): F[Option[(Set[TopicPartitionOffset], Option[FiniteDuration])]] =
    results.set(Vector.empty) *> step { s =>
      if (due(s)) {
        val (renewing, renewal) = s.takeRenewal(at)
        (renewing, Some(renewal))
      } else (s, None)
    } { (consumer, taken) =>
      taken.map { renewal =>
        recordIn(consumer, renewal.acks)
        val sent = renewal.records.collect { case (p, record) if renewedIn(consumer, record) => p }
        consumer.commitAsync()
        sent.toSet -> acquisitionLock(consumer)
      }
    }

  /**
    * Commits, so that Java runs the callback, and returns what it reported, or `None` if
    * unsubscribed. A failed commit counts as no answer yet.
    */
  override def checkRenewals: F[Option[CallbackResults]] =
    locked(withConsumer.blocking { consumer =>
      consumer.commitAsync()
      consumer.subscription().isEmpty
    }).handleError(_ => false)
      .flatMap(unsubscribed => results.getAndSet(Vector.empty).map(r => if (unsubscribed) None else Some(r)))

  /**
    * Polls the confirmed records back and holds them again; the others are lost. Returned records
    * which are not held are released in the same locked section, so that no poll finds them
    * unacknowledged. If the poll is closed, the acknowledgements of the records taken back are left
    * to the next poll, the final commit or the end of `consume`, which wait for the renewal.
    */
  override def takeBack(confirmed: Set[TopicPartitionOffset]): F[Unit] =
    locked {
      for {
        acks <- state.modify(_.takeUnsent)
        polled <- withConsumer.blocking(pollRenewed(_, acks, confirmed))
        at <- F.monotonic
        unknown <- state.modify(_.endRenewal(polled._1, polled._2, at))
        _ <- withConsumer.blocking(release(_, unknown)).whenA(unknown.nonEmpty)
      } yield ()
    }

  /**
    * Only commits if there is something to send, as in implicit mode a commit would accept the
    * poll.
    */
  private def sending(take: ShareConsumerState => (ShareConsumerState, Acks)): F[Unit] =
    step(take) { (consumer, acks) =>
      if (acks.nonEmpty) {
        recordIn(consumer, acks)
        consumer.commitAsync()
      }
    }

  /**
    * Gives Java the pending acknowledgements, and then runs `give`, which sends them.
    */
  private def sendingUnsent[A](give: KafkaByteShareConsumer => A): F[A] =
    step(_.takeUnsent) { (consumer, acks) =>
      recordIn(consumer, acks)
      give(consumer)
    }

  /**
    * Takes from the state and gives to Java, in one locked section.
    */
  private def step[A, B](take: ShareConsumerState => (ShareConsumerState, A))(
    give: (KafkaByteShareConsumer, A) => B
  ): F[B] =
    locked(state.modify(take).flatMap(a => withConsumer.blocking(give(_, a))))

  /**
    * Runs `use` under the mutex and uncancelable, so that nothing else uses the Java consumer
    * meanwhile.
    */
  private def locked[A](use: F[A]): F[A] =
    mutex.lock.surround(use).uncancelable

}

private[kafka] object ShareConsumerCore {

  def of[F[_]](
    withConsumer: WithShareConsumer[F],
    results: Ref[F, CallbackResults]
  )(implicit F: Async[F]): F[ShareConsumerCore[F]] =
    (Mutex[F], SignallingRef.of[F, ShareConsumerState](ShareConsumerState.empty))
      .mapN(new ShareConsumerCore(withConsumer, _, _, results))

  private def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] =
    consumer.acquisitionLockTimeoutMs().toScala.map(_.intValue.millis)

  private def pollRecords(
    consumer: KafkaByteShareConsumer,
    timeout: FiniteDuration
  ): (Vector[KafkaByteConsumerRecord], Option[FiniteDuration]) =
    consumer.poll(timeout.toJava).iterator().asScala.toVector -> acquisitionLock(consumer)

  /**
    * Gives Java the pending acknowledgements, which its poll requires, and polls without fetching,
    * as no renewal is pending anymore. Nothing is polled if no renewal was confirmed, or if
    * unsubscribed.
    */
  private def pollRenewed(
    consumer: KafkaByteShareConsumer,
    acks: Acks,
    confirmed: Set[TopicPartitionOffset]
  ): (Vector[KafkaByteConsumerRecord], Option[FiniteDuration]) = {
    recordIn(consumer, acks)
    if (confirmed.isEmpty || consumer.subscription().isEmpty) (Vector.empty, None)
    else pollRecords(consumer, Duration.Zero)
  }

  private def release(consumer: KafkaByteShareConsumer, records: Vector[KafkaByteConsumerRecord]): Unit = {
    recordIn(consumer, records.map(_ -> AcknowledgeType.Release))
    consumer.commitAsync()
  }

  /**
    * Gives every acknowledgement to Java, then throws the first error other than the
    * `IllegalStateException` for a record Java no longer holds: one failure must not lose the rest.
    */
  private def recordIn(consumer: KafkaByteShareConsumer, acks: Acks): Unit =
    acks
      .map { case (record, kind) => Either.catchNonFatal(consumer.acknowledge(record, kind.asJava)) }
      .collectFirst { case Left(error) if !error.isInstanceOf[IllegalStateException] => error }
      .foreach(error => throw error)

  /**
    * Whether Java took the renewal: it refuses a record it no longer holds.
    */
  private def renewedIn(consumer: KafkaByteShareConsumer, record: KafkaByteConsumerRecord): Boolean =
    Either
      .catchOnly[IllegalStateException](consumer.acknowledge(record, AcknowledgeType.Renew.asJava))
      .isRight

}
