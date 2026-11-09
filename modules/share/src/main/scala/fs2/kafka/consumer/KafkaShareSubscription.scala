/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import cats.Reducible
import cats.data.NonEmptyList
import scala.collection.immutable.SortedSet

trait KafkaShareSubscription[F[_]] {

  /**
    * Subscribes the share consumer to the specified topics. Note that you have to use one of the
    * `subscribe` functions to subscribe to one or more topics before consuming, or a
    * [[NotSubscribedException]] will be raised.
    */
  def subscribeTo(firstTopic: String, remainingTopics: String*): F[Unit] =
    subscribe(NonEmptyList.of(firstTopic, remainingTopics*))

  /**
    * Subscribes the share consumer to the specified topics. Note that you have to use one of the
    * `subscribe` functions to subscribe to one or more topics before consuming, or a
    * [[NotSubscribedException]] will be raised.
    *
    * @param topics
    *   the topics to which the consumer should subscribe
    */
  def subscribe[G[_]: Reducible](topics: G[String]): F[Unit]

  /**
    * Unsubscribes the consumer from all topics. With explicit acknowledgement, the pending
    * acknowledgements are sent first, waiting up to the commit timeout.
    */
  def unsubscribe: F[Unit]

  def subscription: F[SortedSet[String]]

}
