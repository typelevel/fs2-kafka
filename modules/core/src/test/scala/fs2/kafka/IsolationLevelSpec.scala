/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

final class IsolationLevelSpec extends BaseSpec {
  describe("IsolationLevel") {
    it("should have toString matching name") {
      assert(IsolationLevel.ReadCommitted.toString == "ReadCommitted")
      assert(IsolationLevel.ReadUncommitted.toString == "ReadUncommitted")
    }
  }
}
