/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Reducible
import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.Resource
import cats.effect.instances.spawn.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.kafka.consumer.KafkaShareSubscription
import fs2.kafka.consumer.MkShareConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import scala.collection.immutable.SortedSet

/**
  * Share consumers of the same share group, each with its own Java consumer and loop, so that polls
  * are processed in parallel across consumers, one at a time within each.
  *
  * @note
  *   this API is experimental, as are Kafka's share groups.
  */
sealed abstract class KafkaShareConsumers[F[_], K, V] extends KafkaShareSubscription[F] {

  /**
    * The share consumers, in the order they were created.
    */
  def consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]]

  /**
    * Runs [[KafkaShareConsumer.consume]] on every consumer until [[stopConsuming]]. If one fails,
    * the others are canceled, which releases the records they hold, and the error is raised.
    */
  def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit]

  /**
    * Runs [[KafkaShareConsumer.autoConsume]] on every consumer until [[stopConsuming]]. If one
    * fails, the others are canceled and the error is raised. Each of them can then no longer
    * consume, as its next poll would accept the records it was processing: closing the consumers
    * releases them.
    */
  def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit]

  /**
    * Stops every consumer, see [[KafkaShareConsumer.stopConsuming]]. Stopping is final: consuming
    * again fails with an `IllegalStateException`; create new consumers instead.
    */
  def stopConsuming: F[Unit]

  /**
    * The metrics of each consumer, by its `client.id`.
    */
  def metrics: F[Map[String, Map[MetricName, Metric]]]

}

object KafkaShareConsumers {

  /**
    * Creates `consumers` share consumers with the same settings. With more than one, a client id
    * gets the consumer's index as a suffix.
    */
  def resource[F[_], K, V](settings: ShareConsumerSettings[F, K, V], consumers: Int)(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Resource[F, KafkaShareConsumers[F, K, V]] =
    if (consumers < 1)
      Resource.raiseError[F, KafkaShareConsumers[F, K, V], Throwable](
        new IllegalArgumentException(s"consumers must be positive, was $consumers")
      )
    else
      NonEmptyVector
        .fromVectorUnsafe(Vector.range(0, consumers))
        .parTraverse(index =>
          KafkaShareConsumer.resource(if (consumers > 1) indexed(settings, index) else settings)
        )
        .map(new Impl(_))

  private def indexed[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    index: Int
  ): ShareConsumerSettings[F, K, V] =
    settings.properties
      .get(ConsumerConfig.CLIENT_ID_CONFIG)
      .fold(settings)(id => settings.withClientId(s"$id-$index"))

  final private class Impl[F[_], K, V](
    override val consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]]
  )(implicit F: Async[F])
    extends KafkaShareConsumers[F, K, V] {

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      consumers.traverse_(_.subscribe(topics))

    /**
      * Every consumer unsubscribes, even if one fails.
      */
    override def unsubscribe: F[Unit] =
      consumers
        .parTraverse(_.unsubscribe.attempt)
        .flatMap(_.toVector.collectFirst { case Left(error) => error }.traverse_(F.raiseError[Unit]))

    override def subscription: F[SortedSet[String]] =
      consumers.traverse(_.subscription).map(_.reduceLeft(_ ++ _))

    override def consume(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    ): F[Unit] = consumers.parTraverse_(_.consume(process))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      consumers.parTraverse_(_.autoConsume(process))

    override def stopConsuming: F[Unit] = consumers.traverse_(_.stopConsuming)

    /**
      * Java tags each consumer's metrics with its client id, generated if not configured.
      */
    override def metrics: F[Map[String, Map[MetricName, Metric]]] =
      consumers.toVector.zipWithIndex
        .traverse { case (consumer, index) =>
          consumer.metrics.map { metrics =>
            val clientId = metrics.keys.flatMap(name => Option(name.tags.get("client-id"))).headOption
            clientId.getOrElse(index.toString) -> metrics
          }
        }
        .map(_.toMap)

  }

}
