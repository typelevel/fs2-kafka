/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class UnexpectedTopicExceptionSpec extends BaseSpec {
  describe("UnexpectedTopicException") {
    it("should have expected message and toString") {
      val exception = UnexpectedTopicException("topic")
      assert(
        exception.getMessage == "unexpected topic [topic]" &&
          exception.toString == "fs2.kafka.UnexpectedTopicException: unexpected topic [topic]"
      )
    }
  }
}
