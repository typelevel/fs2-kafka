/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.testkit

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.internal.syntax.*
import fs2.kafka.vulcan.SchemaRegistryClientSettings

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import munit.FunSuite
import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility
import org.apache.avro.SchemaCompatibility.Incompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import vulcan.Codec

trait CompatibilityChecker[F[_]] {

  def checkReaderCompatibility[A](
    reader: Codec[A],
    writerSubject: String
  ): F[SchemaCompatibility.SchemaPairCompatibility]

  def checkWriterCompatibility[A](
    writer: Codec[A],
    readerSubject: String
  ): F[SchemaCompatibility.SchemaPairCompatibility]

}

trait AssertableCompatibilityChecker[F[_]] extends CompatibilityChecker[F] {

  def assertReaderCompatibility[A](reader: Codec[A], writerSubject: String): F[Unit]

  def assertWriterCompatibility[A](writer: Codec[A], readerSubject: String): F[Unit]

}

trait SchemaSuite extends FunSuite {

  private def codecAsSchema[A](codec: Codec[A]) = codec.schema.fold(e => fail(e.message), ok => ok)

  private def renderIncompatibilities(incompatibilities: List[Incompatibility]): String =
    "Schema incompatibilities:\n" + incompatibilities
      .zipWithIndex
      .map { case (incompatibility, i) =>
        s"""${i + 1}) ${incompatibility.getType} - ${incompatibility.getMessage}
           |At ${incompatibility.getLocation}
           |Reader schema fragment: ${incompatibility.getReaderFragment.toString}
           |Writer schema fragment: ${incompatibility.getWriterFragment.toString}""".stripMargin
      }
      .mkString("\n-----\n")

  def compatibilityChecker(
    clientSettings: SchemaRegistryClientSettings[IO],
    name: String = "schema-compatibility-checker"
  ) = new Fixture[AssertableCompatibilityChecker[IO]](name) {

    private var checker: AssertableCompatibilityChecker[IO] = null

    override def apply(): AssertableCompatibilityChecker[IO] = checker

    override def beforeAll(): Unit =
      checker = newCompatibilityChecker(clientSettings).unsafeRunSync()

  }

  def newCompatibilityChecker(
    clientSettings: SchemaRegistryClientSettings[IO]
  ): IO[AssertableCompatibilityChecker[IO]] =
    clientSettings
      .createSchemaRegistryClient
      .map { client =>
        new AssertableCompatibilityChecker[IO] {

          private def registrySchema(subject: String): IO[Schema] =
            for {
              metadata <- IO.delay(client.getLatestSchemaMetadata(subject))
              schema   <- IO.delay(
                          client.getSchemaById(metadata.getId).asInstanceOf[AvroSchema]
                        )
            } yield schema.rawSchema()

          def checkReaderCompatibility[A](
            reader: Codec[A],
            writerSubject: String
          ): IO[SchemaCompatibility.SchemaPairCompatibility] = {
            val vulcanSchema = codecAsSchema(reader)
            registrySchema(writerSubject).map { regSchema =>
              SchemaCompatibility.checkReaderWriterCompatibility(
                vulcanSchema,
                regSchema
              )
            }
          }

          def checkWriterCompatibility[A](
            writer: Codec[A],
            readerSubject: String
          ): IO[SchemaCompatibility.SchemaPairCompatibility] = {
            val vulcanSchema = codecAsSchema(writer)
            registrySchema(readerSubject).map { regSchema =>
              SchemaCompatibility.checkReaderWriterCompatibility(
                regSchema,
                vulcanSchema
              )
            }
          }

          def assertWriterCompatibility[A](
            writer: Codec[A],
            readerSubject: String
          ): IO[Unit] =
            checkReaderCompatibility(writer, readerSubject).flatMap { compat =>
              IO.delay {
                assertEquals(
                  compat.getResult.getCompatibility,
                  SchemaCompatibilityType.COMPATIBLE,
                  renderIncompatibilities(compat.getResult.getIncompatibilities.toList)
                )
              }
            }

          def assertReaderCompatibility[A](
            reader: Codec[A],
            writerSubject: String
          ): IO[Unit] =
            checkReaderCompatibility(reader, writerSubject).flatMap { compat =>
              IO.delay {
                assertEquals(
                  compat.getResult.getCompatibility,
                  SchemaCompatibilityType.COMPATIBLE,
                  renderIncompatibilities(compat.getResult.getIncompatibilities.toList)
                )
              }
            }

        }
      }

}
