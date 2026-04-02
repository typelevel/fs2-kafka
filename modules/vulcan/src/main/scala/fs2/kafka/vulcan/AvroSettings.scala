/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.Sync
import cats.syntax.all.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import vulcan.Codec

/**
  * Describes how to create a `KafkaAvroDeserializer` and a `KafkaAvroSerializer` and which settings
  * should be used. Settings are tailored for the Confluent Kafka Avro serializers and
  * deserializers.<br><br>
  *
  * Use `AvroSettings.apply` to create an instance.
  */
sealed abstract class AvroSettings[F[_]] {

  /**
    * The `SchemaRegistryClient` to use for the serializers and deserializers created from this
    * [[AvroSettings]].
    */
  def schemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Creates a new `AvroSettings` instance with the specified setting for whether serializers
    * should register schemas automatically or not.<br><br>
    *
    * The default value is `true`.
    */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F]

  /**
    * Creates a new `AvroSettings` instance with the specified key subject name strategy. This is
    * the class name of the strategy which should be used.<br><br>
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withKeySubjectNameStrategy(keySubjectNameStrategy: String): AvroSettings[F]

  /**
    * Creates a new `AvroSettings` instance with the specified value subject name strategy. This is
    * the class name of the strategy which should be used.<br><br>
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withValueSubjectNameStrategy(valueSubjectNameStrategy: String): AvroSettings[F]

  /**
    * Properties provided when creating a Confluent Kafka Avro serializer or deserializer. Functions
    * in [[AvroSettings]] add properties here as necessary.
    */
  def properties: Map[String, String]

  /**
    * Creates a new [[AvroSettings]] instance including a property with the specified key and value.
    */
  def withProperty(key: String, value: String): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance including properties with the specified keys and
    * values.
    */
  def withProperties(properties: (String, String)*): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance including properties with the specified keys and
    * values.
    */
  def withProperties(properties: Map[String, String]): AvroSettings[F]

  /**
    * Creates a new `KafkaAvroDeserializer` using the settings contained within this
    * [[AvroSettings]] instance, and the specified `isKey` flag, denoting whether a record key or
    * value is being deserialized.
    */
  def createAvroDeserializer(isKey: Boolean): F[(KafkaAvroDeserializer, SchemaRegistryClient)]

  /**
    * Register a schema for a given `Codec` for some type `A`, or return the existing schema id if
    * it already exists.
    * @param subject
    *   The subject name
    * @return
    *   The schema id
    */
  def registerSchema[A](subject: String)(implicit codec: Codec[A]): F[Int]

  /**
    * Creates a new `KafkaAvroSerializer` using the settings contained within this [[AvroSettings]]
    * instance, and the specified `isKey` flag, denoting whether a record key or value is being
    * serialized.
    */
  def createAvroSerializer(
    isKey: Boolean,
    writerSchema: Option[Schema]
  ): F[(KafkaAvroSerializer, SchemaRegistryClient)]

  /**
    * Creates a new [[AvroSettings]] instance with the specified function for creating
    * `KafkaAvroDeserializer`s from settings. The arguments are [[schemaRegistryClient]], `isKey`,
    * and [[properties]].
    */
  def withCreateAvroDeserializer(
    // format: off
    createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)]
    // format: on
  ): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance with the specified function for creating
    * `KafkaAvroSerializer`s from settings. The arguments are [[schemaRegistryClient]], `isKey`, and
    * [[properties]].
    */
  def withCreateAvroSerializer(
    // format: off
    createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Option[Schema], Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)]
    // format: on
  ): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance with the specified function for registering schemas
    * from settings. The arguments are [[schemaRegistryClient]], `subject`, and `codec`.
    */
  def withRegisterSchema(
    registerSchemaWith: (F[SchemaRegistryClient], String, Codec[?]) => F[Int]
  ): AvroSettings[F]

}

object AvroSettings {

  final private[this] case class AvroSettingsImpl[F[_]](
    override val schemaRegistryClient: F[SchemaRegistryClient],
    override val properties: Map[String, String],
    // format: off
    createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)],
    createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Option[Schema], Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)],
    registerSchemaWith: (F[SchemaRegistryClient], String, Codec[?]) => F[Int]
    // format: on
  ) extends AvroSettings[F] {

    override def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F] =
      withProperty("auto.register.schemas", autoRegisterSchemas.toString)

    override def withKeySubjectNameStrategy(keySubjectNameStrategy: String): AvroSettings[F] =
      withProperty("key.subject.name.strategy", keySubjectNameStrategy)

    override def withValueSubjectNameStrategy(valueSubjectNameStrategy: String): AvroSettings[F] =
      withProperty("value.subject.name.strategy", valueSubjectNameStrategy)

    override def withProperty(key: String, value: String): AvroSettings[F] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): AvroSettings[F] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): AvroSettings[F] =
      copy(properties = this.properties ++ properties)

    override def createAvroDeserializer(
      isKey: Boolean
    ): F[(KafkaAvroDeserializer, SchemaRegistryClient)] =
      createAvroDeserializerWith(schemaRegistryClient, isKey, properties)

    override def createAvroSerializer(
      isKey: Boolean,
      writerSchema: Option[Schema]
    ): F[(KafkaAvroSerializer, SchemaRegistryClient)] =
      createAvroSerializerWith(schemaRegistryClient, isKey, writerSchema, properties)

    override def registerSchema[A](subject: String)(implicit codec: Codec[A]): F[Int] =
      registerSchemaWith(schemaRegistryClient, subject, codec)

    override def withCreateAvroDeserializer(
      // format: off
      createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)]
      // format: on
    ): AvroSettings[F] =
      copy(createAvroDeserializerWith = createAvroDeserializerWith)

    override def withCreateAvroSerializer(
      // format: off
      createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Option[Schema], Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)]
      // format: on
    ): AvroSettings[F] =
      copy(createAvroSerializerWith = createAvroSerializerWith)

    override def withRegisterSchema(
      registerSchemaWith: (F[SchemaRegistryClient], String, Codec[?]) => F[Int]
    ): AvroSettings[F] =
      copy(registerSchemaWith = registerSchemaWith)

    override def toString: String =
      "AvroSettings$" + System.identityHashCode(this)

  }

  private[this] def withDefaults(properties: Map[String, String]) =
    properties.updatedIfAbsent("schema.registry.url", "").asJava

  private[this] def create[F[_]](schemaRegistryClient: F[SchemaRegistryClient])(implicit
    F: Sync[F]
  ): AvroSettings[F] =
    AvroSettingsImpl(
      schemaRegistryClient = schemaRegistryClient,
      properties = Map.empty,
      createAvroDeserializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
            deserializer.configure(withDefaults(properties), isKey)
            (deserializer, schemaRegistryClient)
          }
        },
      createAvroSerializerWith = (schemaRegistryClient, isKey, schema, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val serializer = schema match {
              case None         => new KafkaAvroSerializer(schemaRegistryClient)
              case Some(schema) =>
                new KafkaAvroSerializer(schemaRegistryClient) {

                  // Overrides the default auto-registration behaviour, which attempts to guess the
                  // writer schema based on the encoded representation used by the Java Avro SDK.
                  // This works for types such as Records, which contain a reference to the exact schema
                  // that was used to write them, but doesn't work so well for unions (where
                  // the default behaviour is to register just the schema for the alternative
                  // being produced) or logical types such as timestamp-millis (where the logical
                  // type is lost).
                  val parsedSchema = new AvroSchema(schema.toString)

                  override def serialize(topic: String, record: AnyRef): Array[Byte] = {
                    if (record == null) {
                      return null
                    }
                    serializeImpl(
                      getSubjectName(topic, this.isKey, record, parsedSchema),
                      record,
                      parsedSchema
                    )
                  }

                }
            }
            serializer.configure(withDefaults(properties), isKey)
            (serializer, schemaRegistryClient)
          }
        },
      registerSchemaWith = (schemaRegistryClient, subjectName, codec) => {
        schemaRegistryClient.flatMap { client =>
          codec
            .schema
            .leftMap(_.throwable)
            .liftTo[F]
            .flatMap { schema =>
              F.delay(client.register(subjectName, new AvroSchema(schema)))
            }
        }
      }
    )

  def apply[F[_]](
    schemaRegistryClientSettings: SchemaRegistryClientSettings[F]
  )(implicit F: Sync[F]): AvroSettings[F] =
    create(schemaRegistryClientSettings.createSchemaRegistryClient)

  def apply[F[_]](
    schemaRegistryClient: SchemaRegistryClient
  )(implicit F: Sync[F]): AvroSettings[F] =
    create(F.pure(schemaRegistryClient))

}
