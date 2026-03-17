/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.Show
import fs2.kafka.security.KafkaCredentialStore

import org.apache.kafka.clients.admin.AdminClientConfig

/**
  * [[AdminClientSettings]] contain settings necessary to create a [[KafkaAdminClient]]. Several
  * convenience functions are provided so that you don't have to work with `String` values and keys
  * from `AdminClientConfig`. It's still possible to set `AdminClientConfig` values with functions
  * like [[withProperty]].<br><br>
  *
  * [[AdminClientSettings]] instances are immutable and all modification functions return a new
  * [[AdminClientSettings]] instance.<br><br>
  *
  * Use [[AdminClientSettings#apply]] for the default settings, and then apply any desired
  * modifications on top of that instance.
  */
sealed abstract class AdminClientSettings {

  /**
    * Properties which can be provided when creating a Java `KafkaAdminClient` instance. Numerous
    * functions in [[AdminClientSettings]] add properties here if the settings are used by the Java
    * `KafkaAdminClient`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified bootstrap servers. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified client id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * AdminClientConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified reconnect backoff. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG
    * }}}
    */
  def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified max reconnect backoff. This
    * is equivalent to setting the following property using the [[withProperty]] function, except
    * you can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG
    * }}}
    */
  def withReconnectBackoffMax(reconnectBackoffMax: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified retry backoff. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RETRY_BACKOFF_MS_CONFIG
    * }}}
    */
  def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified max connection idle time.
    * This is equivalent to setting the following property using the [[withProperty]] function,
    * except you can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG
    * }}}
    */
  def withConnectionsMaxIdle(connectionsMaxIdle: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified request timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified max metadata age. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.METADATA_MAX_AGE_CONFIG
    * }}}
    */
  def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified retries. This is equivalent
    * to setting the following property using the [[withProperty]] function, except you can specify
    * it with an `Int` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RETRIES_CONFIG
    * }}}
    */
  def withRetries(retries: Int): AdminClientSettings

  /**
    * Includes a property with the specified `key` and `value`. The key should be one of the keys in
    * `AdminClientConfig`, and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): AdminClientSettings

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `AdminClientConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): AdminClientSettings

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `AdminClientConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): AdminClientSettings

  /**
    * The time to wait for the Java `KafkaAdminClient` to shutdown.<br><br>
    *
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[AdminClientSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): AdminClientSettings

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): AdminClientSettings

}

object AdminClientSettings {

  final private[this] case class AdminClientSettingsImpl(
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration
  ) extends AdminClientSettings {

    override def withBootstrapServers(bootstrapServers: String): AdminClientSettings =
      withProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withClientId(clientId: String): AdminClientSettings =
      withProperty(AdminClientConfig.CLIENT_ID_CONFIG, clientId)

    override def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,
        reconnectBackoff.toMillis.toString
      )

    override def withReconnectBackoffMax(reconnectBackoffMax: FiniteDuration): AdminClientSettings =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
        reconnectBackoffMax.toMillis.toString
      )

    override def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings =
      withProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis.toString)

    override def withConnectionsMaxIdle(connectionsMaxIdle: FiniteDuration): AdminClientSettings =
      withProperty(
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
        connectionsMaxIdle.toMillis.toString
      )

    override def withRequestTimeout(requestTimeout: FiniteDuration): AdminClientSettings =
      withProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings =
      withProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAge.toMillis.toString)

    override def withRetries(retries: Int): AdminClientSettings =
      withProperty(AdminClientConfig.RETRIES_CONFIG, retries.toString)

    override def withProperty(key: String, value: String): AdminClientSettings =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): AdminClientSettings =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): AdminClientSettings =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): AdminClientSettings =
      copy(closeTimeout = closeTimeout)

    override def withCredentials(credentialsStore: KafkaCredentialStore): AdminClientSettings =
      withProperties(credentialsStore.properties)

    override def toString: String =
      s"AdminClientSettings(closeTimeout = $closeTimeout)"

  }

  @deprecated("use the overload that takes an argument for BootstrapServers", "2.0.0")
  def apply: AdminClientSettings =
    AdminClientSettingsImpl(
      properties = Map.empty,
      closeTimeout = 20.seconds
    )

  def apply(bootstrapServers: String): AdminClientSettings =
    AdminClientSettingsImpl(
      properties = Map.empty,
      closeTimeout = 20.seconds
    ).withBootstrapServers(bootstrapServers)

  implicit def adminClientSettingsShow[F[_]]: Show[AdminClientSettings] =
    Show.fromToString

}
