/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.unsafe.implicits.global
import cats.effect.IO

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.*

final class AvroSettingsSpec extends AnyFunSpec with ScalaCheckPropertyChecks {

  describe("AvroSettings") {
    it("should provide withAutoRegisterSchemas") {
      forAll { (value: Boolean) =>
        assert {
          settings
            .withAutoRegisterSchemas(value)
            .properties
            .get("auto.register.schemas")
            .contains(value.toString)
        }
      }
    }

    it("should provide withKeySubjectNameStrategy") {
      forAll { (value: String) =>
        assert {
          settings
            .withKeySubjectNameStrategy(value)
            .properties
            .get("key.subject.name.strategy")
            .contains(value)
        }
      }
    }

    it("should provide withValueSubjectNameStrategy") {
      forAll { (value: String) =>
        assert {
          settings
            .withValueSubjectNameStrategy(value)
            .properties
            .get("value.subject.name.strategy")
            .contains(value)
        }
      }
    }

    it("should provide withProperty") {
      forAll { (key: String, value: String) =>
        settings.withProperty(key, value).properties.get(key).contains(value)
      }
    }

    it("should provide withProperties") {
      forAll { (key: String, value: String) =>
        settings.withProperties(key -> value).properties.get(key).contains(value)
      }
    }

    it("should provide withProperties(Map)") {
      forAll { (key: String, value: String) =>
        settings.withProperties(Map(key -> value)).properties.get(key).contains(value)
      }
    }

    it("should provide withCreateAvroDeserializer") {
      assert {
        settings
          .withCreateAvroDeserializer { case _ => IO.raiseError(new RuntimeException) }
          .createAvroDeserializer(isKey = false)
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide withCreateAvroSerializer") {
      assert {
        settings
          .withCreateAvroSerializer((_, _, _, _) => IO.raiseError(new RuntimeException))
          .createAvroSerializer(isKey = false, null)
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide withRegisterSchema") {
      assert {
        settings
          .withRegisterSchema { case _ => IO.raiseError(new RuntimeException) }
          .registerSchema[String]("example-key")
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide toString") {
      assert {
        settings.toString.startsWith("AvroSettings$")
      }
    }
  }

  val settings: AvroSettings[IO] =
    AvroSettings(SchemaRegistryClientSettings[IO]("baseUrl"))

  val settingsWithClient: AvroSettings[IO] =
    AvroSettings(null: SchemaRegistryClient)

}
