/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class ShareAcquireModeSpec extends BaseSpec {
  describe("ShareAcquireMode") {
    it("should have toString matching name") {
      assert(ShareAcquireMode.RecordLimit.toString == "RecordLimit")
      assert(ShareAcquireMode.BatchOptimized.toString == "BatchOptimized")
    }
  }
}
