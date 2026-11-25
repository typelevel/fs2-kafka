/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.TopicPartition

final class RecordNotInFlightExceptionSpec extends BaseSpec {
  describe("RecordNotInFlightException") {
    it("should have expected message and toString") {
      val exception = RecordNotInFlightException(new TopicPartition("topic", 0), 1L)

      assert {
        exception.getMessage == "record at offset 1 of topic-0 is not in flight on this share consumer" &&
        exception.toString == "fs2.kafka.RecordNotInFlightException: record at offset 1 of topic-0 is not in flight on this share consumer"
      }
    }
  }
}
