/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.IO
import cats.effect.Ref
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.AcknowledgeType
import fs2.kafka.BaseSpec
import fs2.kafka.Deserializer
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.ShareConsumerSettings
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.CommitCallback.CallbackResults
import fs2.kafka.internal.ShareConsumerState.Entry
import fs2.kafka.internal.ShareConsumerState.Renewing
import fs2.kafka.internal.ShareConsumerState.offsetOf
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global

/**
  * The scheduler's timing, in virtual time. The core is replaced by one without a Java consumer,
  * which runs the real state transitions and notes when each action was called.
  */
final class ShareConsumerSchedulerSpec extends BaseSpec {

  private val r0: KafkaByteConsumerRecord =
    new ConsumerRecord("topic", 0, 0L, Array.emptyByteArray, Array.emptyByteArray)

  private val lock = 30.seconds

  private val settings =
    ShareConsumerSettings(Deserializer[IO, String], Deserializer[IO, String])

  final private class TestCore(
    state: SignallingRef[IO, ShareConsumerState],
    called: Ref[IO, Vector[(String, FiniteDuration)]],
    started: FiniteDuration,
    subscription: Ref[IO, Boolean],
    failure: Ref[IO, Option[Throwable]]
  ) extends ScheduledCore[IO] {

    // Notes the call with the time since the test started, and fails it if a failure is set.
    private def call(name: String): IO[Unit] =
      IO.monotonic.flatMap(at => called.update(_ :+ (name -> (at - started)))) *>
        failure.getAndSet(None).flatMap(_.traverse_(IO.raiseError[Unit]))

    def calls: IO[Vector[(String, FiniteDuration)]] = called.get

    def calls(name: String): IO[Vector[FiniteDuration]] =
      called.get.map(_.collect { case (`name`, at) => at })

    def failNext(error: Throwable): IO[Unit] = failure.set(Some(error))

    def unsubscribe: IO[Unit] = subscription.set(false)

    def hold: IO[Unit] =
      IO.monotonic.flatMap(now =>
        state.update(_.hold(Vector(offsetOf(r0) -> Delivery(r0, 1, now)), now, Some(lock)))
      )

    def acknowledge(kind: AcknowledgeType): IO[Unit] =
      IO.monotonic
        .flatMap(now => state.modify(_.acknowledge(Chunk(Acknowledgement(offsetOf(r0), 1, kind)), now)))
        .void

    def close: IO[Unit] =
      IO.monotonic.flatMap(now => state.update(_.close(AcknowledgeType.Release, now)))

    override def held: Signal[IO, HeldRecords] = state.map(s => s: HeldRecords)

    override def subscribed: IO[Boolean] = subscription.get

    override def send: IO[Unit] =
      call("send") *> state.modify(_.takeUnsent).void

    override def sendOverdue(max: FiniteDuration, kind: AcknowledgeType): IO[Unit] =
      call(s"sendOverdue($kind)") *> IO.monotonic
        .flatMap(now => state.modify(_.takeOverdue(max, kind, now)))
        .void

    override def renew(at: FiniteDuration)(
      due: HeldRecords => Boolean
    ): IO[Option[(Set[TopicPartitionOffset], Option[FiniteDuration])]] =
      call("renew") *> state.modify { s =>
        if (due(s)) {
          val (renewing, renewal) = s.takeRenewal(at)
          (renewing, Some(renewal.records.map(_._1).toSet -> s.lock))
        } else (s, None)
      }

    // The broker confirms every renewal by the first check.
    override def checkRenewals: IO[Option[CallbackResults]] =
      state.get.map(s => Some(s.records.toVector.collect { case (p, Entry(_, Renewing(_))) => p -> true }))

    override def takeBack(confirmed: Set[TopicPartitionOffset]): IO[Unit] =
      call("takeBack") *> IO.monotonic.flatMap { now =>
        state.modify { s =>
          val returned = s.records.toVector.collect { case (p, e) if confirmed(p) => e.delivery.record }
          s.endRenewal(returned, None, now)
        }.void
      }

    override def skipRenewal(at: FiniteDuration): IO[Unit] =
      call("skipRenewal") *> state.update(_.skipRenewal(at))

    override def failRenewal: IO[Unit] =
      call("failRenewal") *> IO.monotonic.flatMap(now => state.update(_.failRenewal(now)))

  }

  /**
    * Runs `test` in virtual time while the scheduler runs.
    */
  private def scheduled[A](
    settings: ShareConsumerSettings[IO, String, String],
    pause: Signal[IO, Boolean] = Signal.constant[IO, Boolean](false)
  )(test: (TestCore, ShareConsumerScheduler[IO, String, String]) => IO[A]): A =
    TestControl
      .executeEmbed(
        for {
          state <- SignallingRef.of[IO, ShareConsumerState](ShareConsumerState.empty)
          called <- Ref.of[IO, Vector[(String, FiniteDuration)]](Vector.empty)
          subscription <- Ref.of[IO, Boolean](true)
          failure <- Ref.of[IO, Option[Throwable]](None)
          started <- IO.monotonic
          core = new TestCore(state, called, started, subscription, failure)
          config <- ShareConsumerConfig.from(settings)
          scheduler <- ShareConsumerScheduler.of(config, core, pause)
          result <- scheduler.run.background.surround(test(core, scheduler))
        } yield result
      )
      .unsafeRunSync()

  describe("ShareConsumerScheduler") {
    it("should send pending acknowledgements after the max acknowledgement delay") {
      val (before, after) = scheduled(settings.withMaxAcknowledgementDelay(1.second)) { (core, _) =>
        for {
          _ <- core.hold *> core.acknowledge(AcknowledgeType.Accept)
          _ <- IO.sleep(999.millis)
          before <- core.calls
          _ <- IO.sleep(2.millis)
          after <- core.calls
        } yield (before, after)
      }

      before shouldBe empty
      after shouldBe Vector("send" -> 1.second)
    }

    it("should send at once while paused") {
      val calls = scheduled(settings, Signal.constant[IO, Boolean](true)) { (core, _) =>
        core.hold *> core.acknowledge(AcknowledgeType.Accept) *> IO.sleep(1.milli) *> core.calls
      }

      calls shouldBe Vector("send" -> Duration.Zero)
    }

    it("should not send for a closed poll") {
      val calls = scheduled(settings) { (core, _) =>
        core.hold *> core.close *> IO.sleep(1.minute) *> core.calls
      }

      calls shouldBe empty
    }

    it("should acknowledge a record past the max processing time") {
      val calls = scheduled(settings.withMaxProcessingTime(5.seconds)) { (core, _) =>
        core.hold *> IO.sleep(1.minute) *> core.calls
      }

      calls shouldBe Vector("sendOverdue(Release)" -> 5.seconds)
    }

    it("should acknowledge a record past the max processing time with the failure acknowledgement") {
      val overdue =
        settings.withMaxProcessingTime(5.seconds).withFailureAcknowledgement(AcknowledgeType.Reject)
      val calls = scheduled(overdue) { (core, _) =>
        core.hold *> IO.sleep(1.minute) *> core.calls
      }

      calls shouldBe Vector("sendOverdue(Reject)" -> 5.seconds)
    }

    it("should renew the renewal margin before the lock ends, and again after each renewal") {
      val renewals = scheduled(settings.withAutoRenewal(true).withRenewalMargin(5.seconds)) { (core, _) =>
        core.hold *> IO.sleep(1.minute) *> core.calls("renew")
      }

      renewals shouldBe Vector(25.seconds, 50.seconds)
    }

    it("should renew at once for each renewal by the user") {
      val renewals = scheduled(settings) { (core, _) =>
        for {
          _ <- core.hold *> core.acknowledge(AcknowledgeType.Renew)
          _ <- IO.sleep(100.millis)
          _ <- core.acknowledge(AcknowledgeType.Renew)
          _ <- IO.sleep(100.millis)
          renewals <- core.calls("renew")
        } yield renewals
      }

      renewals shouldBe Vector(Duration.Zero, 100.millis)
    }

    it("should skip a renewal while unsubscribed") {
      val calls = scheduled(settings.withAutoRenewal(true).withRenewalMargin(5.seconds)) { (core, _) =>
        core.unsubscribe *> core.hold *> IO.sleep(1.minute) *> core.calls
      }

      calls shouldBe Vector("skipRenewal" -> 25.seconds, "skipRenewal" -> 50.seconds)
    }

    it("should fail consume and retry when an action fails") {
      val (failed, sends) = scheduled(settings.withMaxAcknowledgementDelay(1.second)) { (core, scheduler) =>
        for {
          failing <- scheduler.failed.attempt.start
          _ <- core.failNext(new RuntimeException("boom"))
          _ <- core.hold *> core.acknowledge(AcknowledgeType.Accept)
          _ <- IO.sleep(1.minute)
          sends <- core.calls("send")
          failed <- failing.joinWithNever
        } yield (failed, sends)
      }

      failed.left.toOption.map(_.getMessage) shouldBe Some("boom")
      sends shouldBe Vector(1.second, 2.seconds)
    }

    it("should hold the records again and retry when a renewal fails") {
      val calls = scheduled(settings) { (core, _) =>
        core.failNext(new RuntimeException("boom")) *> core.hold *>
          core.acknowledge(AcknowledgeType.Renew) *> IO.sleep(1.minute) *> core.calls
      }

      calls shouldBe Vector(
        "renew" -> Duration.Zero,
        "failRenewal" -> Duration.Zero,
        "renew" -> 1.second,
        "takeBack" -> (1.second + 10.millis)
      )
    }
  }

}
