/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class CommitTimeoutExceptionSpec extends BaseSpec {
  describe("CommitTimeoutException") {
    it("should have expected message and toString") {
      val exception =
        CommitTimeoutException(
          10.seconds,
          Map(
            new TopicPartition("topic", 0) -> new OffsetAndMetadata(0L),
            new TopicPartition("topic", 1) -> new OffsetAndMetadata(1L)
          )
        )

      assert {
        exception
          .getMessage == "offset commit timeout after 10 seconds for offsets: topic-0 -> 0, topic-1 -> 1" &&
        exception
          .toString == "fs2.kafka.CommitTimeoutException: offset commit timeout after 10 seconds for offsets: topic-0 -> 0, topic-1 -> 1"
      }
    }
  }
}
