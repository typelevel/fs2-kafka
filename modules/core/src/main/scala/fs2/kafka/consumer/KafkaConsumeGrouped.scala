/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import fs2.*
import fs2.kafka.CommittableConsumerRecord

import org.apache.kafka.common.TopicPartition

trait KafkaConsumeGrouped[F[_], K, V] {

  /**
    * `Stream` where each element contains a `Map` of the current partition-group assignment. Keys
    * are partition groups (`Set[TopicPartition]`); values are record streams for that
    * group.<br><br>
    *
    * Assigned partitions are split into up to `maxParallelism` groups, sized as evenly as possible.
    * The default (`Int.MaxValue`) gives one stream per partition.<br><br>
    *
    * With the default assignor, all previous partitions are revoked at once, and a new set of
    * partitions is assigned to a consumer on each rebalance. In this case, each returned `Map`
    * contains the full partition assignment for the consumer, and all streams from the previous
    * assignment are closed. It means that `groupedPartitionsMapStream` reflects the default
    * assignment process in a streaming manner.<br><br>
    *
    * This may not be the case when a custom assignor is configured in the consumer. When using the
    * `CooperativeStickyAssignor`, for instance, partitions may be revoked individually. In this
    * case, each emitted `Map` reflects the current partition-group layout. Streams for retained
    * groups remain active; only streams for revoked or realigned groups are closed.<br><br>
    *
    * This is the most generic `Stream` method. If you don't need per-partition streams, consider
    * using `partitionedStream` or `stream` instead.
    *
    * @note
    *   you have to first use `subscribe` or `assign` to subscribe the consumer before using this
    *   `Stream`. If you forgot to subscribe, there will be a [[NotSubscribedException]] raised in
    *   the `Stream`.
    * @see
    *   [[records]]
    * @see
    *   [[partitionedRecords]]
    */
  def groupedPartitionsMapStream
    : Stream[F, Map[Set[TopicPartition], Stream[F, CommittableConsumerRecord[F, K, V]]]]

}
