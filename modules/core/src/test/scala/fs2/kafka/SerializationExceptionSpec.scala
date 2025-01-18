/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class SerializationExceptionSpec extends BaseSpec {
  describe("SerializationException") {
    it("should have expected message and toString") {
      val exception = SerializationException("message")
      assert(
        exception.getMessage == "message" &&
          exception.toString == "fs2.kafka.SerializationException: message"
      )
    }
  }
}
