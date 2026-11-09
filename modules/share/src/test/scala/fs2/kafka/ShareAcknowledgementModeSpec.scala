/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class ShareAcknowledgementModeSpec extends BaseSpec {
  describe("ShareAcknowledgementMode") {
    it("should have toString matching name") {
      assert(ShareAcknowledgementMode.Explicit.toString == "Explicit")
      assert(ShareAcknowledgementMode.Implicit.toString == "Implicit")
    }
  }
}
