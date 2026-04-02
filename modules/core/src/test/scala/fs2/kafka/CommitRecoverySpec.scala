/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.FiniteDuration

import cats.data.Chain
import cats.effect.{IO, Ref, Temporal}
import cats.effect.kernel.{Deferred, Fiber, Poll}
import cats.effect.unsafe.implicits.global
import cats.syntax.functor.*

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, RetriableCommitFailedException}
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.TopicPartition

final class CommitRecoverySpec extends BaseAsyncSpec {

  describe("CommitRecovery#Default") {

    List(new RetriableCommitFailedException("retriable"), new RebalanceInProgressException())
      .foreach { ex =>
        it(s"should retry $ex with jittered exponential backoff and fixed rate") {
          val (result: Either[Throwable, Unit], sleeps: Chain[FiniteDuration]) =
            Ref
              .of[IO, Chain[FiniteDuration]](Chain.empty)
              .flatMap { ref =>
                implicit val temporal: Temporal[IO] = storeSleepsTemporal(ref)
                val commit: IO[Unit]                = IO.raiseError(ex)
                val offsets                         = Map(new TopicPartition("topic", 0) -> new OffsetAndMetadata(1))
                val recovery                        = CommitRecovery.Default.recoverCommitWith(offsets, commit)
                val attempted                       = commit.handleErrorWith(recovery).attempt
                attempted.flatMap(ref.get.tupleLeft)
              }
              .unsafeRunSync()

          assert {
            result
              .left
              .toOption
              .map(_.toString)
              .contains {
                s"fs2.kafka.CommitRecoveryException: offset commit is still failing after 15 attempts for offsets: topic-0 -> 1; last exception was: $ex"
              }
          }

          assert(sleeps.size == 15L)

          assert {
            sleeps
              .toList
              .take(10)
              .zipWithIndex
              .forall { case (sleep, attempt) =>
                val max = 10 * Math.pow(2, attempt.toDouble + 1)
                0 <= sleep.toMillis && sleep.toMillis < max
              }
          }

          assert(sleeps.toList.drop(10).forall(_.toMillis == 10000L))
        }
      }

    it("should not recover non-retriable exceptions") {
      val commit: IO[Unit] = IO.raiseError(new RuntimeException("commit"))
      val retry: IO[Unit]  = IO.raiseError(new RuntimeException("retry"))
      val recovery         = CommitRecovery.Default.recoverCommitWith(Map(), retry)
      val result           = commit.handleErrorWith(recovery).attempt.unsafeRunSync()
      assert(result.left.toOption.map(_.getMessage).contains("commit"))
    }

    it("should have Default as String representation") {
      assert(CommitRecovery.Default.toString == "Default")
    }
  }

  describe("CommitRecovery#None") {
    it("should not recover any exceptions") {
      val commit: IO[Unit] = IO.raiseError(new RuntimeException("commit"))
      val retry: IO[Unit]  = IO.raiseError(new RuntimeException("retry"))
      val recovery         = CommitRecovery.None.recoverCommitWith(Map(), retry)
      val result           = commit.handleErrorWith(recovery).attempt.unsafeRunSync()
      assert(result.left.toOption.map(_.getMessage).contains("commit"))
    }

    it("should have None as String representation") {
      assert(CommitRecovery.None.toString == "None")
    }
  }

  implicit val jitter: Jitter[IO] =
    Jitter.default[IO].unsafeRunSync()

  private def storeSleepsTemporal(_ref: Ref[IO, Chain[FiniteDuration]]): Temporal[IO] =
    new Temporal[IO] {

      override def sleep(time: FiniteDuration): IO[Unit] = _ref.update(_.append(time))

      override def ref[A](a: A): IO[Ref[IO, A]] = ???

      override def deferred[A]: IO[Deferred[IO, A]] = ???

      override def start[A](fa: IO[A]): IO[Fiber[IO, Throwable, A]] = ???

      override def never[A]: IO[A] = ???

      override def cede: IO[Unit] = ???

      override def forceR[A, B](fa: IO[A])(fb: IO[B]): IO[B] = ???

      override def uncancelable[A](body: Poll[IO] => IO[A]): IO[A] = ???

      override def canceled: IO[Unit] = ???

      override def onCancel[A](fa: IO[A], fin: IO[Unit]): IO[A] = ???

      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
        IO.asyncForIO.tailRecM(a)(f)

      override def raiseError[A](e: Throwable): IO[A] = IO.raiseError(e)

      override def handleErrorWith[A](fa: IO[A])(f: Throwable => IO[A]): IO[A] =
        fa.handleErrorWith(f)

      override def pure[A](x: A): IO[A] = IO.pure(x)

      override def monotonic: IO[FiniteDuration] = ???

      override def realTime: IO[FiniteDuration] = ???

      override def unique: IO[cats.effect.kernel.Unique.Token] = ???

    }

}
