package fs2.kafka.internal.actor

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

sealed abstract class Request[F[_], -K, -V]

object Request {

  final case class WithPermit[F[_], A](fa: F[A], callback: Either[Throwable, A] => F[Unit])
      extends Request[F, Any, Any]

  final case class Poll[F[_]]() extends Request[F, Any, Any]

  private[this] val pollInstance: Poll[Nothing] =
    Poll[Nothing]()

  def poll[F[_]]: Poll[F] =
    pollInstance.asInstanceOf[Poll[F]]

  final case class Commit[F[_]](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    callback: Either[Throwable, Unit] => Unit
  ) extends Request[F, Any, Any]

  final case class ManualCommitSync[F[_]](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    callback: Either[Throwable, Unit] => F[Unit]
  ) extends Request[F, Any, Any]

}
