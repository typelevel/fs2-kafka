/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class ImplicitAcknowledgementExceptionSpec extends BaseSpec {
  describe("ImplicitAcknowledgementException") {
    it("should have expected message and toString") {
      val exception = ImplicitAcknowledgementException()

      assert {
        exception.getMessage == "records cannot be acknowledged with share.acknowledgement.mode=implicit: use autoConsume" &&
        exception.toString == "fs2.kafka.ImplicitAcknowledgementException: records cannot be acknowledged with share.acknowledgement.mode=implicit: use autoConsume"
      }
    }
  }
}
