/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import java.util.UUID

import munit.FunSuite

class KafkaMessageKeySuite extends FunSuite {

  final private class UnknownKey(val value: String)
  final private class CustomKey(val value: String)

  test("string instance preserves the key value and omits null") {
    val instance = KafkaMessageKey[String]

    assertEquals(instance.toMessageKey("key"), Some("key"))
    assertEquals(instance.toMessageKey(null), None)
  }

  test("built-in scalar instances render canonical string values") {
    assertEquals(KafkaMessageKey[Int].toMessageKey(42), Some("42"))
    assertEquals(KafkaMessageKey[Long].toMessageKey(42L), Some("42"))
    assertEquals(
      KafkaMessageKey[UUID].toMessageKey(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
      Some("123e4567-e89b-12d3-a456-426614174000")
    )
  }

  test("boxed numeric keys fall back to omission") {
    assertEquals(KafkaMessageKey[java.lang.Integer].toMessageKey(42: java.lang.Integer), None)
    assertEquals(KafkaMessageKey[java.lang.Long].toMessageKey(42L: java.lang.Long), None)
    assertEquals(KafkaMessageKey[UUID].toMessageKey(null), None)
  }

  test("fallback instance omits unknown key types") {
    assertEquals(
      KafkaMessageKey[UnknownKey].toMessageKey(new UnknownKey("secret")),
      None
    )
  }

  test("custom instance can define key rendering") {
    implicit val customKafkaMessageKey: KafkaMessageKey[CustomKey] =
      KafkaMessageKey.instance(key => Some(s"custom:${key.value}"))

    assertEquals(
      KafkaMessageKey[CustomKey].toMessageKey(new CustomKey("value")),
      Some("custom:value")
    )
  }

  test("contramap can derive wrapper instances from an existing renderer") {
    implicit val customKafkaMessageKey: KafkaMessageKey[CustomKey] =
      KafkaMessageKey[String].contramap(_.value)

    assertEquals(
      KafkaMessageKey[CustomKey].toMessageKey(new CustomKey("value")),
      Some("value")
    )
  }

}
