/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext

import cats.effect.Resource
import cats.Show
import fs2.kafka.security.KafkaCredentialStore

import org.apache.kafka.clients.producer.ProducerConfig

/**
  * [[ProducerSettings]] contain settings necessary to create a [[KafkaProducer]]. At the very
  * least, this includes a key serializer and a value serializer.<br><br>
  *
  * Several convenience functions are provided so that you don't have to work with `String` values
  * and `ProducerConfig` for configuration. It's still possible to specify `ProducerConfig` values
  * with functions like [[withProperty]].<br><br>
  *
  * [[ProducerSettings]] instances are immutable and all modification functions return a new
  * [[ProducerSettings]] instance.<br><br>
  *
  * Use `ProducerSettings#apply` to create a new instance.
  */
sealed abstract class ProducerSettings[F[_], K, V] {

  /**
    * The `Serializer` to use for serializing record keys.
    */
  def keySerializer: Resource[F, KeySerializer[F, K]]

  /**
    * The `Serializer` to use for serializing record values.
    */
  def valueSerializer: Resource[F, ValueSerializer[F, V]]

  /**
    * Replace the serializers with those provided in the arguments.
    */
  def withSerializers[K1, V1](
    keySerializer: Resource[F, KeySerializer[F, K1]],
    valueSerializer: Resource[F, ValueSerializer[F, V1]]
  ): ProducerSettings[F, K1, V1]

  /**
    * A custom [[ExecutionContext]] to use for blocking Kafka operations. If not provided, the
    * default blocking ExecutionContext provided by [[cats.effect.Sync]] will be used.
    */
  def customBlockingContext: Option[ExecutionContext]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified [[ExecutionContext]] to use for
    * blocking operations.
    *
    * If not provided, the default blocking ExecutionContext provided by [[cats.effect.Sync]] will
    * be used. If in doubt, leave this unset.
    */
  def withCustomBlockingContext(ec: ExecutionContext): ProducerSettings[F, K, V]

  /**
    * Properties which can be provided when creating a Java `KafkaProducer` instance. Numerous
    * functions in [[ProducerSettings]] add properties here if the settings are used by the Java
    * `KafkaProducer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified bootstrap servers. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified acknowledgements. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with [[Acks]] instead of a `String`.
    *
    * {{{
    * ProducerConfig.ACKS_CONFIG
    * }}}
    */
  def withAcks(acks: Acks): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified batch size. This is equivalent
    * to setting the following property using the [[withProperty]] function, except you can specify
    * it with an `Int` instead of a `String`.
    *
    * {{{
    * ProducerConfig.BATCH_SIZE_CONFIG
    * }}}
    */
  def withBatchSize(batchSize: Int): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified client id. This is equivalent
    * to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified retries. This is equivalent to
    * setting the following property using the [[withProperty]] function, except you can specify it
    * with an `Int` instead of a `String`.
    *
    * {{{
    * ProducerConfig.RETRIES_CONFIG
    * }}}
    */
  def withRetries(retries: Int): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified max in-flight requests per
    * connection. This is equivalent to setting the following property using the [[withProperty]]
    * function, except you can specify it with an `Int` instead of a `String`.
    *
    * {{{
    * ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
    * }}}
    */
  def withMaxInFlightRequestsPerConnection(
    maxInFlightRequestsPerConnection: Int
  ): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified idempotence setting. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
    * }}}
    */
  def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified linger. This is equivalent to
    * setting the following property using the [[withProperty]] function, except you can specify it
    * with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.LINGER_MS_CONFIG
    * }}}
    */
  def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified request timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified delivery timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Includes a property with the specified `key` and `value`. The key should be one of the keys in
    * `ProducerConfig`, and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ProducerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ProducerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ProducerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ProducerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ProducerSettings[F, K, V]

  /**
    * The time to wait for the Java `KafkaProducer` to shutdown.<br><br>
    *
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ProducerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified transaction timeout. This is
    * setting the following producer property, except you can specify it with a `FiniteDuration`
    * instead of a `String`.
    *
    * {{{
    * ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
    * }}}
    */
  def withTransactionTimeout(transactionTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified transactional id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.TRANSACTIONAL_ID_CONFIG
    * }}}
    */
  def withTransactionalId(transactionalId: String): ProducerSettings[F, K, V]

  /**
    * Controls whether [[fs2.kafka.KafkaProducer.produce]] fails immediately if any
    * [[org.apache.kafka.clients.producer.KafkaProducer.send]] callback resolves with error.
    *
    * When set to `true`, the `produce` method will fail fast, returning an error as soon as any
    * record in the [[ProducerRecords]] fails to be sent.
    *
    * The default value is `false`, meaning the `produce` method will not fail fast and will
    * continue processing other records even if some callbacks fail.
    */
  def failFastProduce: Boolean

  /**
    * Creates a new [[ProducerSettings]] with the specified [[failFastProduce]].
    */
  def withFailFastProduce(failFastProduce: Boolean): ProducerSettings[F, K, V]

}

object ProducerSettings {

  final private[this] case class ProducerSettingsImpl[F[_], K, V](
    override val keySerializer: Resource[F, KeySerializer[F, K]],
    override val valueSerializer: Resource[F, ValueSerializer[F, V]],
    override val customBlockingContext: Option[ExecutionContext],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val failFastProduce: Boolean
  ) extends ProducerSettings[F, K, V] {

    override def withCustomBlockingContext(ec: ExecutionContext): ProducerSettings[F, K, V] =
      copy(customBlockingContext = Some(ec))

    override def withBootstrapServers(bootstrapServers: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings[F, K, V] =
      withProperty(
        ProducerConfig.ACKS_CONFIG,
        acks match {
          case Acks.ZeroAcks => "0"
          case Acks.OneAcks  => "1"
          case Acks.AllAcks  => "all"
        }
      )

    override def withBatchSize(batchSize: Int): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings[F, K, V] =
      withProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.LINGER_MS_CONFIG, linger.toMillis.toString)

    override def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString)

    override def withProperty(key: String, value: String): ProducerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      copy(closeTimeout = closeTimeout)

    /**
      * Includes the credentials properties from the provided [[KafkaCredentialStore]]
      */
    override def withCredentials(
      credentialsStore: KafkaCredentialStore
    ): ProducerSettings[F, K, V] =
      withProperties(credentialsStore.properties)

    override def toString: String =
      s"ProducerSettings(closeTimeout = $closeTimeout)"

    override def withSerializers[K1, V1](
      keySerializer: Resource[F, KeySerializer[F, K1]],
      valueSerializer: Resource[F, ValueSerializer[F, V1]]
    ): ProducerSettings[F, K1, V1] =
      copy(keySerializer = keySerializer, valueSerializer = valueSerializer)

    override def withFailFastProduce(failFastProduce: Boolean): ProducerSettings[F, K, V] =
      copy(failFastProduce = failFastProduce)

    override def withTransactionTimeout(
      transactionTimeout: FiniteDuration
    ): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout.toMillis.toString)

    override def withTransactionalId(transactionalId: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)

  }

  private[this] def create[F[_], K, V](
    keySerializer: Resource[F, KeySerializer[F, K]],
    valueSerializer: Resource[F, ValueSerializer[F, V]]
  ): ProducerSettings[F, K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      customBlockingContext = None,
      properties = Map.empty,
      closeTimeout = 60.seconds,
      failFastProduce = false
    )

  def apply[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V]
  ): ProducerSettings[F, K, V] =
    create(
      keySerializer = Resource.pure(keySerializer),
      valueSerializer = Resource.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: Resource[F, KeySerializer[F, K]],
    valueSerializer: ValueSerializer[F, V]
  ): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer,
      valueSerializer = Resource.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: Resource[F, ValueSerializer[F, V]]
  ): ProducerSettings[F, K, V] =
    create(
      keySerializer = Resource.pure(keySerializer),
      valueSerializer = valueSerializer
    )

  def apply[F[_], K, V](implicit
    keySerializer: Resource[F, KeySerializer[F, K]],
    valueSerializer: Resource[F, ValueSerializer[F, V]]
  ): ProducerSettings[F, K, V] =
    create(keySerializer, valueSerializer)

  implicit def producerSettingsShow[F[_], K, V]: Show[ProducerSettings[F, K, V]] =
    Show.fromToString

}
