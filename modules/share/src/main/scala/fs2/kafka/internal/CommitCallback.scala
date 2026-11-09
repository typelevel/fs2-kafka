/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Ref
import cats.effect.std.Dispatcher
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.converters.collection.*
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.TopicIdPartition

private[kafka] object CommitCallback {

  /**
    * Each offset Java reported as sent, with whether the broker accepted it.
    */
  type CallbackResults = Vector[(TopicPartitionOffset, Boolean)]

  /**
    * Java's commit callback, adding what it reports of the sent acknowledgements, renewals
    * included, to `results`. Java runs it during a poll, commit or close.
    */
  def apply[F[_]](
    dispatcher: Dispatcher[F],
    results: Ref[F, CallbackResults]
  ): AcknowledgementCommitCallback =
    new AcknowledgementCommitCallback {
      override def onComplete(
        offsets: java.util.Map[TopicIdPartition, java.util.Set[java.lang.Long]],
        exception: Exception
      ): Unit = {
        val reported = offsets.asScala.toVector.flatMap { case (partition, sent) =>
          sent.asScala.toVector.map(offset =>
            (partition.topicPartition -> offset.longValue) -> (exception == null)
          )
        }
        dispatcher.unsafeRunSync(results.update(_ ++ reported))
      }
    }

}
