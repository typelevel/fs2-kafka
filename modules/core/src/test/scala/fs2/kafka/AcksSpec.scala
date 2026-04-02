/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class AcksSpec extends BaseSpec {
  describe("Acks") {
    it("should have toString matching name") {
      assert(Acks.Zero.toString == "Zero")
      assert(Acks.One.toString == "One")
      assert(Acks.All.toString == "All")
    }
  }
}
