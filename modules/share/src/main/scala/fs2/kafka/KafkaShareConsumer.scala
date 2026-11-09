/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Reducible
import cats.effect.Async
import cats.effect.Outcome
import cats.effect.Poll
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.consumer.KafkaMetrics
import fs2.kafka.consumer.KafkaShareSubscription
import fs2.kafka.consumer.MkShareConsumer
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.Acknowledger
import fs2.kafka.internal.CommitCallback
import fs2.kafka.internal.CommitCallback.CallbackResults
import fs2.kafka.internal.Delivery
import fs2.kafka.internal.ShareConsumerConfig
import fs2.kafka.internal.ShareConsumerCore
import fs2.kafka.internal.ShareConsumerScheduler
import fs2.kafka.internal.ShareConsumerState.offsetOf
import fs2.kafka.internal.WithShareConsumer
import fs2.kafka.internal.converters.option.*
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.TimeoutException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
  * [[KafkaShareConsumer]] represents a consumer of Kafka records in a share group. Records are
  * processed with [[consume]] or [[autoConsume]], one poll at a time, until
  * [[stopConsuming]].<br><br>
  *
  * Acknowledgements are recorded by the consumer, and sent by a background scheduler when due, or
  * with the next poll. The scheduler also renews acquisition locks, and acknowledges records held
  * past the max processing time with the failure acknowledgement.
  *
  * @note
  *   this API is experimental, as are Kafka's share groups.
  */
sealed abstract class KafkaShareConsumer[F[_], K, V] extends KafkaShareSubscription[F] with KafkaMetrics[F] {

  /**
    * Returns the settings used to create the consumer instance.
    */
  def settings: ShareConsumerSettings[F, K, V]

  /**
    * Consumes records with explicit acknowledgement, running `process` on each poll until
    * [[stopConsuming]]. `process` may acknowledge its records in parallel, as each one
    * finishes.<br><br>
    *
    * Records left unacknowledged when `process` returns or fails get the `failureAcknowledgement`,
    * and are released if it is canceled. Acknowledging a record after `process` returned fails with
    * a [[RecordNotInFlightException]].<br><br>
    *
    * After [[stopConsuming]], the last acknowledgements are awaited, and fail `consume` if the
    * broker rejects them. Others are not awaited: if they fail, their records are delivered again.
    */
  def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit]

  /**
    * Consumes records with implicit acknowledgement, running `process` on each poll until
    * [[stopConsuming]]. The records of a poll are accepted once `process` returns.<br><br>
    *
    * If `process` fails or is canceled, its records are released when the consumer is closed, and
    * the consumer refuses to consume again, as Java would accept them with its next poll.
    */
  def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit]

  /**
    * Makes [[consume]] or [[autoConsume]] return once the current poll is processed and the last
    * acknowledgements are sent. Stopping is final: consuming again fails with an
    * `IllegalStateException`.
    */
  def stopConsuming: F[Unit]

}

object KafkaShareConsumer {

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Resource` context, using the specified
    * [[ShareConsumerSettings]].<br><br>
    *
    * The `Dispatcher` is created before the Java consumer, as closing it can still run the commit
    * callback.
    */
  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkShareConsumer[F]): Resource[F, KafkaShareConsumer[F, K, V]] =
    for {
      keyDeserializer <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      config <- Resource.eval(ShareConsumerConfig.from(settings))
      dispatcher <- Dispatcher.sequential[F]
      withConsumer <- WithShareConsumer(mk, settings)
      results <- Resource.eval(Ref.of[F, CallbackResults](Vector.empty))
      _ <- Resource.eval(
        withConsumer
          .blocking(_.setAcknowledgementCommitCallback(CommitCallback(dispatcher, results)))
          .unlessA(config.implicitMode)
      )
      core <- Resource.eval(ShareConsumerCore.of(withConsumer, results))
      pause = settings.pauseWhen.getOrElse(Signal.constant[F, Boolean](false))
      scheduler <- Resource.eval(ShareConsumerScheduler.of(config, core, pause))
      lifecycle <- Resource.eval(SignallingRef.of[F, Lifecycle](Idle))
      consumer = new Impl(config, keyDeserializer, valueDeserializer, core, scheduler, lifecycle, pause)
      _ <- scheduler.run.background
    } yield consumer

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Stream` context, using the specified
    * [[ShareConsumerSettings]].
    */
  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  private val subscriptionCheckInterval: FiniteDuration = 50.millis

  final private case class Polled(
    records: Vector[KafkaByteConsumerRecord],
    start: FiniteDuration,
    lock: Option[FiniteDuration]
  )

  /**
    * Stopping is final. In implicit mode, a failed or canceled `process` poisons the consumer, as
    * any later poll or commit would accept its records.
    */
  sealed abstract private class Lifecycle
  private case object Idle extends Lifecycle
  private case object Consuming extends Lifecycle
  private case object Stopped extends Lifecycle
  private case object Poisoned extends Lifecycle

  final private class Impl[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    core: ShareConsumerCore[F],
    scheduler: ShareConsumerScheduler[F, K, V],
    lifecycle: SignallingRef[F, Lifecycle],
    pauseSignal: Signal[F, Boolean]
  )(implicit F: Async[F])
    extends KafkaShareConsumer[F, K, V]
    with Acknowledger[F] {

    override val settings: ShareConsumerSettings[F, K, V] = config.settings

    override def metrics: F[Map[MetricName, Metric]] =
      core.metrics

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      core.subscribe(topics.toList)

    /**
      * In explicit mode, pending acknowledgements are committed first, as Java's `unsubscribe`
      * doesn't send them. In implicit mode, a commit would accept records still being processed.
      */
    override def unsubscribe: F[Unit] =
      if (config.implicitMode) core.unsubscribe
      else
        core.commitAndUnsubscribe(settings.commitTimeout)

    override def subscription: F[SortedSet[String]] =
      core.subscription

    override def stopConsuming: F[Unit] =
      lifecycle.update {
        case Poisoned => Poisoned
        case _ => Stopped
      }

    override def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit] =
      if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
      else exclusively(loop(processed(process)))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit"))
      else exclusively(loop(autoProcessed(process)))

    /**
      * Runs `run` as the only consumption, and fails if the scheduler fails. It is uncancelable
      * until `endConsuming` is in place, so that a cancelation can't leave the consumer consuming.
      */
    private def exclusively(run: F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        startConsuming *>
          cancelable(requireSubscribed *> scheduler.reset *> F.race(scheduler.failed, run).void)
            .guarantee(endConsuming)
      }

    private def startConsuming: F[Unit] =
      lifecycle.modify {
        case Idle => Consuming -> F.unit
        case other => other -> F.raiseError[Unit](new IllegalStateException(refusal(other)))
      }.flatten

    private def requireSubscribed: F[Unit] =
      core.subscribed.ifM(F.unit, F.raiseError(NotSubscribedException()))

    /**
      * Sends what is pending, as no poll follows, once the records being renewed are back, up to
      * the commit timeout, so that an acknowledgement made during a renewal is sent too.
      */
    private def endConsuming: F[Unit] =
      core.awaitRenewals.timeoutTo(settings.commitTimeout, F.unit) *>
        core.send.attempt.void *> lifecycle.update {
          case Consuming => Idle
          case other => other
        }

    private def refusal(lifecycle: Lifecycle): String =
      lifecycle match {
        case Stopped => "the consumer was stopped"
        case Poisoned => "process failed in implicit mode: close the consumer, which releases its records"
        case _ => "the consumer is already consuming"
      }

    /**
      * Polls until stopped, waiting instead while paused, or while records are being renewed.
      * Stopping is checked first, so that it doesn't depend on the pause signal.
      */
    private def loop(run: (Poll[F], Polled) => F[Unit]): F[Unit] =
      lifecycle.get.flatMap {
        case Stopped => finish
        case _ =>
          (pauseSignal.get, core.renewing).flatMapN { (paused, renewing) =>
            val step =
              if (paused) waitForUnpause
              else if (renewing) waitForRenewals
              else pollOnce(run)
            step >> loop(run)
          }
      }

    /**
      * Nothing is polled while paused, so a commit sends what is pending.
      */
    private def waitForUnpause: F[Unit] =
      core.commit *> untilStopped(pauseSignal.waitUntil(!_))

    /**
      * Java's poll would fetch more records while a renewal is pending.
      */
    private def waitForRenewals: F[Unit] =
      untilStopped(core.awaitRenewals)

    private def untilStopped(wait: F[Unit]): F[Unit] =
      F.race(lifecycle.waitUntil(_ == Stopped), wait).void

    /**
      * Uncancelable until `run` has the records, as Java holds them from the poll on.
      */
    private def pollOnce(run: (Poll[F], Polled) => F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        for {
          start <- F.monotonic
          polled <- core.poll(settings.pollTimeout)
          _ <- polled.traverse_ { case (records, lock) =>
            run(cancelable, Polled(records, start, lock)).whenA(records.nonEmpty)
          }
        } yield polled.isDefined
      }.flatMap(subscribed => F.sleep(subscriptionCheckInterval).unlessA(subscribed))

    /**
      * Waits for the renewals, up to the commit timeout, and commits synchronously, raising the
      * first error the broker returned.
      */
    private def finish: F[Unit] =
      for {
        _ <- core.awaitRenewals.timeoutTo(settings.commitTimeout, F.unit)
        rejected <- core.commitSync(settings.commitTimeout).adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }
        _ <- rejected.traverse_(F.raiseError[Unit](_))
      } yield ()

    /**
      * The records are held from the poll on, also if decoding fails or `process` is canceled.
      */
    private def processed(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      hold(polled) *>
        cancelable(Chunk.from(polled.records).traverse(decodeAcknowledgeable).flatMap(process))
          .guaranteeCase(closePoll)

    /**
      * A failure or cancelation poisons the consumer, as Java would accept the records with its
      * next poll.
      */
    private def autoProcessed(
      process: Chunk[ConsumerRecord[K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      cancelable(Chunk.from(polled.records).traverse(decode).flatMap(process))
        .guaranteeCase(outcome => lifecycle.set(Poisoned).unlessA(outcome.isSuccess))

    private def hold(polled: Polled): F[Unit] =
      F.monotonic.flatMap { now =>
        val deliveries = polled.records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
        core.hold(deliveries, polled.start, polled.lock)
      }

    /**
      * Records `process` left get the `failureAcknowledgement`, or `Release` if it was canceled.
      */
    private def closePoll(outcome: Outcome[F, Throwable, Unit]): F[Unit] = {
      val kind = if (outcome.isCanceled) AcknowledgeType.Release else settings.failureAcknowledgement
      core.close(kind)
    }

    private def decode(record: KafkaByteConsumerRecord): F[ConsumerRecord[K, V]] =
      ConsumerRecord.fromJava(record, keyDeserializer, valueDeserializer)

    private def decodeAcknowledgeable(
      record: KafkaByteConsumerRecord
    ): F[AcknowledgeableConsumerRecord[F, K, V]] =
      decode(record).map { decoded =>
        val (partition, offset) = offsetOf(record)
        AcknowledgeableConsumerRecord(
          decoded,
          AcknowledgeableOffset(partition, offset, deliveryCountOf(record), this)
        )
      }

    /**
      * Only records the acknowledgements: Java gets them when they are sent.
      */
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      core
        .acknowledge(acks)
        .flatMap(_.headOption.traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2))))

  }

}
