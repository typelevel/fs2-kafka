/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import _root_.vulcan.Codec

package object vulcan {

  /**
    * Alias for `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`.
    */
  type SchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

  /**
    * Alias for `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient`.
    */
  type CachedSchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

  /**
    * Alias for `io.confluent.kafka.serializers.KafkaAvroDeserializer`.
    */
  type KafkaAvroDeserializer =
    io.confluent.kafka.serializers.KafkaAvroDeserializer

  /**
    * Alias for `io.confluent.kafka.serializers.KafkaAvroSerializer`.
    */
  type KafkaAvroSerializer =
    io.confluent.kafka.serializers.KafkaAvroSerializer

  def avroDeserializer[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  def avroSerializer[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)

}
