/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.Temporal
import cats.syntax.applicativeError.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.Functor

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, RetriableCommitFailedException}
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.TopicPartition

/**
  * [[CommitRecovery]] describes how to recover from exceptions raised while trying to commit
  * offsets. See [[CommitRecovery#Default]] for the default recovery strategy. If you do not wish to
  * recover from any exceptions, you can use [[CommitRecovery#None]].<br><br>
  *
  * To create a new [[CommitRecovery]], simply create a new instance and implement the
  * [[recoverCommitWith]] function with the wanted recovery strategy. To use the [[CommitRecovery]],
  * you can simply set it with [[ConsumerSettings#withCommitRecovery]].
  */
abstract class CommitRecovery {

  /**
    * Describes recovery from offset commit exceptions. The `commit` parameter can be used to retry
    * the commit. Note that if more than one retry is desirable, errors from `commit` will need to
    * be handled and recovered.<br><br>
    *
    * The offsets we are trying to commit are available via the `offsets` parameter. Waiting before
    * retrying again can be done via the provided `Timer` instance, and jitter can be applied using
    * the `Jitter` instance.
    */
  def recoverCommitWith[F[_]](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    commit: F[Unit]
  )(implicit
    F: Temporal[F],
    jitter: Jitter[F]
  ): Throwable => F[Unit]

}

object CommitRecovery {

  /**
    * The default [[CommitRecovery]] used in [[ConsumerSettings]] unless a different one has been
    * specified. The default recovery strategy only retries `RetriableCommitFailedException`s and
    * `RebalanceInProgressException`s. These exceptions are retried with a jittered exponential
    * backoff, where the time in milliseconds before retrying is calculated using:
    *
    * {{{
    * Random.nextDouble() * Math.min(10000, 10 * Math.pow(2, n))
    * }}}
    *
    * where `n` is the retry attempt (first attempt is `n = 1`). This is done for up to 10 attempts,
    * after which we change to retry using a fixed time of 10 seconds, for up to another 5 attempts.
    * If at that point we are still faced with `RetriableCommitFailedException` or
    * `RebalanceInProgressException`, we give up and raise a [[CommitRecoveryException]] with the
    * last such error experienced.<br><br>
    *
    * The sum of time spent waiting between retries will always be less than 70 220 milliseconds, or
    * ~70 seconds. Note that this does not include the time for attempting to commit offsets. Offset
    * commit times are limited with [[ConsumerSettings.commitTimeout]].
    */
  val Default: CommitRecovery =
    new CommitRecovery {

      private[this] def backoff[F[_]](attempt: Int)(implicit
        F: Functor[F],
        jitter: Jitter[F]
      ): F[FiniteDuration] = {
        val millis = Math.min(10000, 10 * Math.pow(2, attempt.toDouble))
        jitter.withJitter(millis).map(_.millis)
      }

      override def recoverCommitWith[F[_]](
        offsets: Map[TopicPartition, OffsetAndMetadata],
        commit: F[Unit]
      )(implicit
        F: Temporal[F],
        jitter: Jitter[F]
      ): Throwable => F[Unit] = {
        def retry(attempt: Int): Throwable => F[Unit] = {
          case retriable @ (_: RetriableCommitFailedException | _: RebalanceInProgressException) =>
            val commitWithRecovery = commit.handleErrorWith(retry(attempt + 1))
            if (attempt <= 10) backoff(attempt).flatMap(F.sleep) >> commitWithRecovery
            else if (attempt <= 15) F.sleep(10.seconds) >> commitWithRecovery
            else F.raiseError(CommitRecoveryException(attempt - 1, retriable, offsets))

          case nonRetriable: Throwable =>
            F.raiseError(nonRetriable)
        }

        retry(attempt = 1)
      }

      override def toString: String =
        "Default"

    }

  /**
    * A [[CommitRecovery]] that does not retry any exceptions.
    */
  val None: CommitRecovery =
    new CommitRecovery {

      override def recoverCommitWith[F[_]](
        offsets: Map[TopicPartition, OffsetAndMetadata],
        commit: F[Unit]
      )(implicit
        F: Temporal[F],
        jitter: Jitter[F]
      ): Throwable => F[Unit] = F.raiseError

      override def toString: String =
        "None"

    }

}
