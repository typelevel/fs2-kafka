/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class DeserializationExceptionSpec extends BaseSpec {
  describe("DeserializationException") {
    it("should have expected message and toString") {
      val exception = DeserializationException("message")
      assert(
        exception.getMessage == "message" &&
          exception.toString == "fs2.kafka.DeserializationException: message"
      )
    }
  }
}
