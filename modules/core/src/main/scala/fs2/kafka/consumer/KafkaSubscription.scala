/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import scala.util.matching.Regex
import cats.data.NonEmptyList
import cats.Reducible
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata

trait KafkaSubscription[F[_]] {

  /**
    * Subscribes the consumer to the specified topics. Note that you have to use one of the
    * `subscribe` functions to subscribe to one or more topics before using any of the provided
    * `Stream`s, or a [[NotSubscribedException]] will be raised in the `Stream`s.
    */
  def subscribeTo(firstTopic: String, remainingTopics: String*): F[Unit] =
    subscribe(NonEmptyList.of(firstTopic, remainingTopics*))

  /**
    * Subscribes the consumer to the specified topics. Note that you have to use one of the
    * `subscribe` functions to subscribe to one or more topics before using any of the provided
    * `Stream`s, or a [[NotSubscribedException]] will be raised in the `Stream`s.
    *
    * @param topics
    *   the topics to which the consumer should subscribe
    */
  def subscribe[G[_]: Reducible](topics: G[String]): F[Unit]

  /**
    * Subscribes the consumer to the topics matching the specified `Regex`. Note that you have to
    * use one of the `subscribe` functions before you can use any of the provided `Stream`s, or a
    * [[NotSubscribedException]] will be raised in the `Stream`s.
    *
    * @param regex
    *   the regex to which matching topics should be subscribed
    */
  def subscribe(regex: Regex): F[Unit]

  /**
    * Unsubscribes the consumer from all topics and partitions assigned by `subscribe` or `assign`.
    */
  def unsubscribe: F[Unit]

  /**
   * Provides the current consumer group metadata.
   * @return
   */
  def groupMetadata: F[ConsumerGroupMetadata]

}
