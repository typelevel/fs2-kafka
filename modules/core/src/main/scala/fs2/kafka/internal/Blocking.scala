/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

import cats.effect.{Async, Resource, Sync}
import cats.effect.syntax.async.*

private[kafka] trait Blocking[F[_]] {
  def apply[A](a: => A): F[A]
}

private[kafka] object Blocking {

  def fromSync[F[_]: Sync]: Blocking[F] = new Blocking[F] {
    override def apply[A](a: => A): F[A] = Sync[F].blocking(a)
  }

  def fromExecutionContext[F[_]](ec: ExecutionContext)(implicit F: Async[F]): Blocking[F] =
    new Blocking[F] {
      def apply[A](a: => A): F[A] = F.delay(a).evalOn(ec)
    }

  def singleThreaded[F[_]](name: String)(implicit F: Async[F]): Resource[F, Blocking[F]] =
    Resource
      .make(
        F.delay(
          Executors.newSingleThreadExecutor { (runnable: Runnable) =>
            val thread = new Thread(runnable)
            thread.setName(s"$name-${thread.getId}")
            thread.setDaemon(true)
            thread
          }
        )
      )(ex => F.delay(ex.shutdown()))
      .map(ex => fromExecutionContext(ExecutionContext.fromExecutor(ex)))

}
