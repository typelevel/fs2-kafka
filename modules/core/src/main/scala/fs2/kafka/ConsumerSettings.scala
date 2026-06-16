/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext
import scala.util.Try

import cats.effect.Resource
import cats.Show
import fs2.kafka.security.KafkaCredentialStore

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.OffsetFetchResponse

/**
  * [[ConsumerSettings]] contain settings necessary to create a [[KafkaConsumer]]. At the very
  * least, this includes key and value deserializers.<br><br>
  *
  * The following consumer configuration defaults are used.<br>
  *   - `auto.offset.reset` is set to `none` to avoid the surprise of the otherwise default `latest`
  *     setting.<br>
  *   - `enable.auto.commit` is set to `false` since offset commits are managed manually.<br><br>
  *
  * Several convenience functions are provided so that you don't have to work with `String` values
  * and `ConsumerConfig` for configuration. It's still possible to specify `ConsumerConfig` values
  * with functions like [[withProperty]].<br><br>
  *
  * [[ConsumerSettings]] instances are immutable and all modification functions return a new
  * [[ConsumerSettings]] instance.<br><br>
  *
  * Use `ConsumerSettings#apply` to create a new instance.
  */
sealed abstract class ConsumerSettings[F[_], K, V] {

  /**
    * The `Deserializer` to use for deserializing record keys.
    */
  def keyDeserializer: Resource[F, KeyDeserializer[F, K]]

  /**
    * The `Deserializer` to use for deserializing record values.
    */
  def valueDeserializer: Resource[F, ValueDeserializer[F, V]]

  /**
    * Creates a new `ConsumerSettings` instance that replaces the serializers with those provided.
    * Note that this will remove any custom `recordMetadata` configuration.
    */
  def withDeserializers[K0, V0](
    keyDeserializer: Resource[F, KeyDeserializer[F, K0]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V0]]
  ): ConsumerSettings[F, K0, V0]

  /**
    * A custom `ExecutionContext` to use for blocking Kafka operations. If not provided, a default
    * single-threaded `ExecutionContext` will be created when creating a `KafkaConsumer` instance.
    */
  def customBlockingContext: Option[ExecutionContext]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified [[ExecutionContext]] to use for
    * blocking operations.
    *
    * Because the underlying Java consumer is not thread-safe, the ExecutionContext *must* be
    * single-threaded. If in doubt, leave this unset so that a default single-threaded blocker will
    * be provided.
    */
  def withCustomBlockingContext(ec: ExecutionContext): ConsumerSettings[F, K, V]

  /**
    * Properties which can be provided when creating a Java `KafkaConsumer` instance. Numerous
    * functions in [[ConsumerSettings]] add properties here if the settings are used by the Java
    * `KafkaConsumer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified bootstrap servers. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified auto offset reset. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with [[AutoOffsetReset]] instead of a `String`.
    *
    * {{{
    * ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
    * }}}
    */
  def withAutoOffsetReset(autoOffsetReset: AutoOffsetReset): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified client id. This is equivalent
    * to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified group id. This is equivalent to
    * setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.GROUP_ID_CONFIG
    * }}}
    */
  def withGroupId(groupId: String): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified group instance id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.GROUP_INSTANCE_ID_CONFIG
    * }}}
    */
  def withGroupInstanceId(groupInstanceId: String): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified max poll records. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with an `Int` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_RECORDS_CONFIG
    * }}}
    */
  def withMaxPollRecords(maxPollRecords: Int): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified max poll interval. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
    * }}}
    */
  def withMaxPollInterval(maxPollInterval: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns value for property:
    *
    * {{{
    * ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
    * }}}
    *
    * Returns a value as a [[FiniteDuration]] for convenience
    */
  def sessionTimeout: FiniteDuration

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified session timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withSessionTimeout(sessionTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified heartbeat interval. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
    * }}}
    */
  def withHeartbeatInterval(heartbeatInterval: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified auto commit setting. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
    * }}}
    *
    * Note that by default, this setting is set to `false`.
    */
  def withEnableAutoCommit(enableAutoCommit: Boolean): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified auto commit interval. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    * }}}
    */
  def withAutoCommitInterval(autoCommitInterval: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified request timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified default api timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withDefaultApiTimeout(defaultApiTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified isolation level. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with an [[IsolationLevel]] instead of a `String`.
    *
    * {{{
    * ConsumerConfig.ISOLATION_LEVEL_CONFIG
    * }}}
    */
  def withIsolationLevel(isolationLevel: IsolationLevel): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified allow auto create topics. This
    * is equivalent to setting the following property using the [[withProperty]] function, except
    * you can specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG
    * }}}
    */
  def withAllowAutoCreateTopics(allowAutoCreateTopics: Boolean): ConsumerSettings[F, K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified client rack. This is equivalent
    * to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_RACK_CONFIG
    * }}}
    */
  def withClientRack(clientRack: String): ConsumerSettings[F, K, V]

  /**
    * Includes a property with the specified `key` and `value`. The key should be one of the keys in
    * `ConsumerConfig`, and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ConsumerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ConsumerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ConsumerSettings[F, K, V]

  /**
    * The time to wait for the Java `KafkaConsumer` to shutdown.<br><br>
    *
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * The time to wait for offset commits to complete. If an offset commit doesn't complete within
    * this time, a [[CommitTimeoutException]] will be raised instead.<br><br>
    *
    * The default value is 15 seconds.
    */
  def commitTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[commitTimeout]].
    */
  def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * How often we should attempt to call `poll` on the Java `KafkaConsumer`.<br><br>
    *
    * The default value is 50 milliseconds.
    */
  def pollInterval: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[pollInterval]].
    */
  def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * How long we should allow the `poll` call to block for in the Java `KafkaConsumer`.<br><br>
    *
    * The default value is 50 milliseconds.
    */
  def pollTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[pollTimeout]].
    */
  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[F, K, V]

  /**
    * The [[CommitRecovery]] strategy for recovering from offset commit exceptions.<br><br>
    *
    * The default is [[CommitRecovery#Default]].
    */
  def commitRecovery: CommitRecovery

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[CommitRecovery]] as the
    * [[commitRecovery]] to use.
    */
  def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[F, K, V]

  /**
    * The function used to specify metadata for records. This metadata will be included in
    * `OffsetAndMetadata` in the [[CommittableOffset]]s, and can then be committed with the
    * offsets.<br><br>
    *
    * By default, there will be no metadata, as determined by `OffsetFetchResponse.NO_METADATA`.
    */
  def recordMetadata: ConsumerRecord[K, V] => String

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[recordMetadata]]. Note that replacing
    * the serializers via `withSerializers` will reset this to the default.
    */
  def withRecordMetadata(recordMetadata: ConsumerRecord[K, V] => String): ConsumerSettings[F, K, V]

  /**
    * The maximum number of record batches to prefetch per topic-partition. This means that, while
    * records are being processed, there can be up to `maxPrefetchBatches * max.poll.records`
    * records per topic-partition that have already been fetched, and are waiting to be processed.
    * You can use [[withMaxPollRecords]] to control the `max.poll.records` setting.<br><br>
    *
    * This setting effectively controls backpressure, i.e. the maximum number of batches to prefetch
    * per topic-partition before starting to slow down (not fetching more records) until processing
    * has caught-up.<br><br>
    *
    * Note that prefetching cannot be disabled and is generally preferred since it yields improved
    * performance. The minimum value for this setting is `2`.
    */
  def maxPrefetchBatches: Int

  /**
    * Creates a new [[ConsumerSettings]] with the specified value for [[maxPrefetchBatches]]. Note
    * that if a value lower than the minimum `2` is specified, [[maxPrefetchBatches]] will instead
    * be set to `2` and not the specified value.
    */
  def withMaxPrefetchBatches(maxPrefetchBatches: Int): ConsumerSettings[F, K, V]

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): ConsumerSettings[F, K, V]

  /**
    * One of two possible modes of operation for [[KafkaConsumer.partitionsMapStream]]. See
    * [[RebalanceRevokeMode]] for detailed explanation of differences between them.
    */
  def rebalanceRevokeMode: RebalanceRevokeMode

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[rebalanceRevokeMode]].
    */
  def withRebalanceRevokeMode(rebalanceRevokeMode: RebalanceRevokeMode): ConsumerSettings[F, K, V]

}

object ConsumerSettings {

  final private[this] case class ConsumerSettingsImpl[F[_], K, V](
    override val keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    override val valueDeserializer: Resource[F, ValueDeserializer[F, V]],
    override val customBlockingContext: Option[ExecutionContext],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val commitTimeout: FiniteDuration,
    override val pollInterval: FiniteDuration,
    override val pollTimeout: FiniteDuration,
    override val commitRecovery: CommitRecovery,
    override val recordMetadata: ConsumerRecord[K, V] => String,
    override val maxPrefetchBatches: Int,
    override val rebalanceRevokeMode: RebalanceRevokeMode
  ) extends ConsumerSettings[F, K, V] {

    override def withCustomBlockingContext(ec: ExecutionContext): ConsumerSettings[F, K, V] =
      copy(customBlockingContext = Some(ec))

    override def withBootstrapServers(bootstrapServers: String): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAutoOffsetReset(autoOffsetReset: AutoOffsetReset): ConsumerSettings[F, K, V] =
      withProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        autoOffsetReset match {
          case AutoOffsetReset.EarliestOffsetReset => "earliest"
          case AutoOffsetReset.LatestOffsetReset   => "latest"
          case AutoOffsetReset.NoneOffsetReset     => "none"
        }
      )

    override def withClientId(clientId: String): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

    override def withGroupId(groupId: String): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    override def withGroupInstanceId(groupInstanceId: String): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)

    override def withMaxPollRecords(maxPollRecords: Int): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)

    override def withMaxPollInterval(maxPollInterval: FiniteDuration): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toMillis.toString)

    // need to use Try, to avoid separate implementation for scala 2.12
    override def sessionTimeout: FiniteDuration =
      properties
        .get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)
        .flatMap(str => Try(str.toLong).toOption)
        .map(_.millis)
        .getOrElse(45000.millis)

    override def withSessionTimeout(sessionTimeout: FiniteDuration): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout.toMillis.toString)

    override def withHeartbeatInterval(
      heartbeatInterval: FiniteDuration
    ): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval.toMillis.toString)

    override def withEnableAutoCommit(enableAutoCommit: Boolean): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)

    override def withAutoCommitInterval(
      autoCommitInterval: FiniteDuration
    ): ConsumerSettings[F, K, V] =
      withProperty(
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        autoCommitInterval.toMillis.toString
      )

    override def withRequestTimeout(requestTimeout: FiniteDuration): ConsumerSettings[F, K, V] =
      withProperty(
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        requestTimeout.toMillis.toString
      )

    override def withDefaultApiTimeout(
      defaultApiTimeout: FiniteDuration
    ): ConsumerSettings[F, K, V] =
      withProperty(
        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
        defaultApiTimeout.toMillis.toString
      )

    override def withIsolationLevel(isolationLevel: IsolationLevel): ConsumerSettings[F, K, V] =
      withProperty(
        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
        isolationLevel match {
          case IsolationLevel.ReadCommittedIsolationLevel   => "read_committed"
          case IsolationLevel.ReadUncommittedIsolationLevel => "read_uncommitted"
        }
      )

    override def withAllowAutoCreateTopics(
      allowAutoCreateTopics: Boolean
    ): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopics.toString)

    override def withClientRack(clientRack: String): ConsumerSettings[F, K, V] =
      withProperty(ConsumerConfig.CLIENT_RACK_CONFIG, clientRack)

    override def withProperty(key: String, value: String): ConsumerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ConsumerSettings[F, K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ConsumerSettings[F, K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[F, K, V] =
      copy(closeTimeout = closeTimeout)

    override def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[F, K, V] =
      copy(commitTimeout = commitTimeout)

    override def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[F, K, V] =
      copy(pollInterval = pollInterval)

    override def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[F, K, V] =
      copy(pollTimeout = pollTimeout)

    override def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[F, K, V] =
      copy(commitRecovery = commitRecovery)

    override def withRecordMetadata(
      recordMetadata: ConsumerRecord[K, V] => String
    ): ConsumerSettings[F, K, V] =
      copy(recordMetadata = recordMetadata)

    override def withMaxPrefetchBatches(maxPrefetchBatches: Int): ConsumerSettings[F, K, V] =
      copy(maxPrefetchBatches = Math.max(2, maxPrefetchBatches))

    override def withCredentials(
      credentialsStore: KafkaCredentialStore
    ): ConsumerSettings[F, K, V] =
      withProperties(credentialsStore.properties)

    override def withRebalanceRevokeMode(
      rebalanceRevokeMode: RebalanceRevokeMode
    ): ConsumerSettings[F, K, V] =
      copy(rebalanceRevokeMode = rebalanceRevokeMode)

    override def toString: String =
      s"ConsumerSettings(closeTimeout = $closeTimeout, commitTimeout = $commitTimeout, pollInterval = $pollInterval, pollTimeout = $pollTimeout, commitRecovery = $commitRecovery)"

    override def withDeserializers[K0, V0](
      keyDeserializer: Resource[F, KeyDeserializer[F, K0]],
      valueDeserializer: Resource[F, ValueDeserializer[F, V0]]
    ): ConsumerSettings[F, K0, V0] =
      copy(
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
        recordMetadata = _ => OffsetFetchResponse.NO_METADATA
      )

  }

  private[this] def create[F[_], K, V](
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]]
  ): ConsumerSettings[F, K, V] =
    ConsumerSettingsImpl(
      customBlockingContext = None,
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer,
      properties = Map(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG  -> "none",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
      ),
      closeTimeout = 20.seconds,
      commitTimeout = 15.seconds,
      pollInterval = 50.millis,
      pollTimeout = 50.millis,
      commitRecovery = CommitRecovery.Default,
      recordMetadata = _ => OffsetFetchResponse.NO_METADATA,
      maxPrefetchBatches = 2,
      rebalanceRevokeMode = RebalanceRevokeMode.Eager
    )

  def apply[F[_], K, V](
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V]
  ): ConsumerSettings[F, K, V] =
    create(
      keyDeserializer = Resource.pure(keyDeserializer),
      valueDeserializer = Resource.pure(valueDeserializer)
    )

  def apply[F[_], K, V](
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: ValueDeserializer[F, V]
  ): ConsumerSettings[F, K, V] =
    create(
      keyDeserializer = keyDeserializer,
      valueDeserializer = Resource.pure(valueDeserializer)
    )

  def apply[F[_], K, V](
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]]
  ): ConsumerSettings[F, K, V] =
    create(
      keyDeserializer = Resource.pure(keyDeserializer),
      valueDeserializer = valueDeserializer
    )

  def apply[F[_], K, V](implicit
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]]
  ): ConsumerSettings[F, K, V] =
    create(
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer
    )

  implicit def consumerSettingsShow[F[_], K, V]: Show[ConsumerSettings[F, K, V]] =
    Show.fromToString

}
