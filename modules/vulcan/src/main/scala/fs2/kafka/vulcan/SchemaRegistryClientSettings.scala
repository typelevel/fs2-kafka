/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.Sync
import cats.Show
import fs2.kafka.internal.converters.collection.*

/**
  * Describes how to create a `SchemaRegistryClient` and which settings should be used. Settings are
  * tailored for default implementation `CachedSchemaRegistryClient`.
  *
  * Use `SchemaRegistryClient#apply` to create an instance.
  */
sealed abstract class SchemaRegistryClientSettings[F[_]] {

  /**
    * The base URL of the schema registry service.
    */
  def baseUrl: String

  /**
    * The maximum number of schemas to cache in the client.
    *
    * The default value is 1000.
    */
  def maxCacheSize: Int

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance with the specified [[maxCacheSize]].
    */
  def withMaxCacheSize(maxCacheSize: Int): SchemaRegistryClientSettings[F]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance with the specified authentication
    * details.
    */
  def withAuth(auth: Auth): SchemaRegistryClientSettings[F]

  /**
    * Properties provided when creating a `SchemaRegistryClient`. Numerous functions in
    * [[SchemaRegistryClientSettings]] add properties here as necessary.
    */
  def properties: Map[String, String]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance including a property with the
    * specified key and value.
    */
  def withProperty(key: String, value: String): SchemaRegistryClientSettings[F]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance including properties with the
    * specified keys and values.
    */
  def withProperties(properties: (String, String)*): SchemaRegistryClientSettings[F]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance including properties with the
    * specified keys and values.
    */
  def withProperties(properties: Map[String, String]): SchemaRegistryClientSettings[F]

  /**
    * Creates a new `SchemaRegistryClient` using the settings contained within this
    * [[SchemaRegistryClientSettings]].
    */
  def createSchemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance with the specified function for
    * creating new instances of `SchemaRegistryClient` from settings. The arguments are [[baseUrl]],
    * [[maxCacheSize]], and [[properties]].
    */
  def withCreateSchemaRegistryClient(
    createSchemaRegistryClientWith: (String, Int, Map[String, String]) => F[SchemaRegistryClient]
  ): SchemaRegistryClientSettings[F]

}

object SchemaRegistryClientSettings {

  final private[this] case class SchemaRegistryClientSettingsImpl[F[_]](
    override val baseUrl: String,
    override val maxCacheSize: Int,
    override val properties: Map[String, String],
    // format: off
    createSchemaRegistryClientWith: (String, Int, Map[String, String]) => F[SchemaRegistryClient]
    // format: on
  ) extends SchemaRegistryClientSettings[F] {

    override def withMaxCacheSize(maxCacheSize: Int): SchemaRegistryClientSettings[F] =
      copy(maxCacheSize = maxCacheSize)

    override def withAuth(auth: Auth): SchemaRegistryClientSettings[F] =
      auth match {
        case Auth.BasicAuth(username, password) =>
          withProperties(
            "basic.auth.credentials.source"        -> "USER_INFO",
            "schema.registry.basic.auth.user.info" -> s"$username:$password"
          )

        case Auth.BearerAuth(token) =>
          withProperties(
            "bearer.auth.credentials.source" -> "STATIC_TOKEN",
            "bearer.auth.token"              -> token
          )

        case Auth.NoAuth =>
          this
      }

    override def withProperty(key: String, value: String): SchemaRegistryClientSettings[F] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): SchemaRegistryClientSettings[F] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): SchemaRegistryClientSettings[F] =
      copy(properties = this.properties ++ properties)

    override def createSchemaRegistryClient: F[SchemaRegistryClient] =
      createSchemaRegistryClientWith(baseUrl, maxCacheSize, properties)

    override def withCreateSchemaRegistryClient(
      createSchemaRegistryClientWith: (
        String,
        Int,
        Map[String, String]
      ) => F[SchemaRegistryClient]
    ): SchemaRegistryClientSettings[F] =
      copy(createSchemaRegistryClientWith = createSchemaRegistryClientWith)

    override def toString: String =
      s"SchemaRegistryClientSettings(baseUrl = $baseUrl, maxCacheSize = $maxCacheSize)"

  }

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance using the specified base URL of the
    * schema registry.
    */
  def apply[F[_]](baseUrl: String)(implicit F: Sync[F]): SchemaRegistryClientSettings[F] =
    SchemaRegistryClientSettingsImpl(
      baseUrl = baseUrl,
      maxCacheSize = 1000,
      properties = Map.empty,
      createSchemaRegistryClientWith = (baseUrl, maxCacheSize, properties) =>
        F.delay(new CachedSchemaRegistryClient(baseUrl, maxCacheSize, properties.asJava))
    )

  implicit def schemaRegistryClientSettingsShow[F[_]]: Show[SchemaRegistryClientSettings[F]] =
    Show.fromToString

}
