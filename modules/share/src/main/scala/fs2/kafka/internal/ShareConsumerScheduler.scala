/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.AcknowledgeType
import fs2.kafka.ShareConsumerSettings
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.CommitCallback.CallbackResults
import scala.concurrent.duration.*

/**
  * What the scheduler needs of the [[ShareConsumerCore]]: the held records, and the actions it runs
  * on them.
  */
private[kafka] trait ScheduledCore[F[_]] {

  def held: Signal[F, HeldRecords]

  def subscribed: F[Boolean]

  def send: F[Unit]

  def sendOverdue(max: FiniteDuration, kind: AcknowledgeType): F[Unit]

  def renew(at: FiniteDuration)(
    due: HeldRecords => Boolean
  ): F[Option[(Set[TopicPartitionOffset], Option[FiniteDuration])]]

  def checkRenewals: F[Option[CallbackResults]]

  def takeBack(confirmed: Set[TopicPartitionOffset]): F[Unit]

  def skipRenewal(at: FiniteDuration): F[Unit]

  def failRenewal: F[Unit]

}

/**
  * Sends acknowledgements when due, renews records, and settles records past the max processing
  * time, for as long as the consumer runs.<br><br>
  *
  * It sleeps until the earliest deadline, recomputed whenever the held records or the pause change.
  * A failure fails the running `consume`, and the action is retried.
  *
  * @param failure
  *   the last failure, which fails the running `consume`
  */
final private[kafka] class ShareConsumerScheduler[F[_], K, V] private (
  config: ShareConsumerConfig[F, K, V],
  core: ScheduledCore[F],
  pause: Signal[F, Boolean],
  failure: SignallingRef[F, Option[Throwable]]
)(implicit F: Async[F]) {

  import ShareConsumerScheduler.*

  private val settings: ShareConsumerSettings[F, K, V] = config.settings

  /**
    * A failure of the deadlines themselves, such as a failing pause signal, is retried as well.
    * A deadline wakes the scheduler when it changes, and whenever it has already passed, as a
    * deadline can come due again unchanged: a user renewal is always due at once.
    */
  def run: F[Unit] =
    retried(
      Signal
        .applicativeInstance[F]
        .map2(core.held, pause)((s, p) => actions(s, p).map(_._1).reduceOption(_ min _))
        .discrete
        .evalMap(deadline => F.monotonic.tupleLeft(deadline))
        .filterWithPrevious { case ((previous, _), (next, now)) => previous != next || next.exists(_ <= now) }
        .map(_._1)
        .switchMap {
          case None => Stream.empty
          case Some(at) =>
            Stream.eval(F.monotonic).flatMap(now => Stream.sleep_[F](at - now)) ++ Stream.emit(())
        }
        .evalMap(_ => retried(due))
        .compile
        .drain
    )

  def failed: F[Unit] =
    failure.discrete
      .collectFirst { case Some(error) => error }
      .compile
      .lastOrError
      .flatMap(F.raiseError[Unit])

  def reset: F[Unit] =
    failure.set(None)

  /**
    * Fails the running `consume` and retries `step` after a failure, as the deadline which made it
    * due might not change.
    */
  private def retried(step: F[Unit]): F[Unit] =
    step.handleErrorWith(error => failure.set(Some(error)) *> F.sleep(retryInterval) *> retried(step))

  /**
    * The actions with their deadlines, in the order they run when due together.
    */
  private def actions(s: HeldRecords, paused: Boolean): List[(FiniteDuration, F[Unit])] =
    List(
      settings.maxProcessingTime.flatMap(max => s.nextOverdue(max).tupleRight(acknowledgeOverdue(max))),
      s.nextRenewal(autoRenewalInterval(s)).tupleRight(renew),
      s.nextSend(settings.maxAcknowledgementDelay, paused).tupleRight(core.send)
    ).flatten

  private def autoRenewalInterval(s: HeldRecords): Option[FiniteDuration] =
    Some(config.renewalInterval(s.lock)).filter(_ => settings.autoRenewal)

  /**
    * Runs the due actions one at a time, recomputing the deadlines after each, as an action can
    * move the others' deadlines.
    */
  private def due: F[Unit] =
    (core.held.get, pause.get, F.monotonic)
      .flatMapN { (s, p, now) =>
        actions(s, p)
          .collectFirst { case (at, action) if at <= now => action.as(true) }
          .getOrElse(F.pure(false))
      }
      .iterateWhile(identity)
      .void

  /**
    * Sent at once, so that a late acknowledgement fails.
    */
  private def acknowledgeOverdue(max: FiniteDuration): F[Unit] =
    core.sendOverdue(max, settings.failureAcknowledgement)

  /**
    * Renews every unacknowledged record as the renewal example in Java's `KafkaShareConsumer`
    * documentation does: the renewals are sent without waiting, `confirmed` waits for the broker's
    * answer, and `core.takeBack` polls the records back. If anything fails, the records are held
    * again as they were. Nothing is renewed while unsubscribed, as Java's poll would refuse to
    * return the records.
    */
  private def renew: F[Unit] =
    for {
      at <- F.monotonic
      subscribed <- core.subscribed
      _ <- if (subscribed) renewAt(at) else core.skipRenewal(at)
    } yield ()

  private def renewAt(at: FiniteDuration): F[Unit] = {
    val renewal =
      for {
        sent <- core.renew(at)(renewalDue(at))
        _ <- sent.traverse_ { case (offsets, lock) =>
          val forgetAt = at + lock.getOrElse(Duration.Zero) + config.apiTimeout
          confirmed(offsets, forgetAt, Set.empty).flatMap(core.takeBack)
        }
      } yield ()
    renewal.onError { case _ => core.failRenewal }
  }

  /**
    * Checked again under the mutex, as a poll since the renewal came due holds new records.
    */
  private def renewalDue(at: FiniteDuration)(s: HeldRecords): Boolean =
    s.nextRenewal(autoRenewalInterval(s)).exists(_ <= at)

  /**
    * Commits until the broker answered for every record sent for renewal, and returns those it
    * confirmed, or none if the broker can't answer anymore or the consumer unsubscribed.
    */
  private def confirmed(
    waiting: Set[TopicPartitionOffset],
    forgetAt: FiniteDuration,
    done: Set[TopicPartitionOffset]
  ): F[Set[TopicPartitionOffset]] =
    F.monotonic.flatMap { now =>
      if (waiting.isEmpty) F.pure(done)
      else if (now >= forgetAt) F.pure(Set.empty[TopicPartitionOffset])
      else
        F.sleep(renewalCheckInterval) *> core.checkRenewals.flatMap {
          case None => F.pure(Set.empty[TopicPartitionOffset])
          case Some(reported) =>
            val answered = reported.filter { case (p, _) => waiting(p) }
            val accepted = answered.collect { case (p, true) => p }
            confirmed(waiting -- answered.map(_._1), forgetAt, done ++ accepted)
        }
    }

}

private[kafka] object ShareConsumerScheduler {

  def of[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    core: ScheduledCore[F],
    pause: Signal[F, Boolean]
  )(implicit F: Async[F]): F[ShareConsumerScheduler[F, K, V]] =
    SignallingRef
      .of[F, Option[Throwable]](None)
      .map(new ShareConsumerScheduler(config, core, pause, _))

  private val retryInterval: FiniteDuration = 1.second

  private val renewalCheckInterval: FiniteDuration = 10.millis

}
