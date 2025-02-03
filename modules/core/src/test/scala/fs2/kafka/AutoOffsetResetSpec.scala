/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class AutoOffsetResetSpec extends BaseSpec {
  describe("AutoOffsetReset") {
    it("should have toString matching name") {
      assert(AutoOffsetReset.Earliest.toString == "Earliest")
      assert(AutoOffsetReset.Latest.toString == "Latest")
      assert(AutoOffsetReset.None.toString == "None")
    }
  }
}
