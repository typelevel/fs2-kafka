/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import cats.effect.Concurrent
import cats.syntax.flatMap.*
import cats.Monad
import fs2.*
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.CommittableOffsetBatch
import fs2.kafka.ConsumerRecord

trait KafkaConsumeChunk[F[_], K, V] extends KafkaConsume[F, K, V] {

  /**
    * Consume from all assigned partitions concurrently, processing the records in `Chunk` s. For
    * each `Chunk`, the provided `processor` is called, after that has finished the offsets for all
    * messages in the chunk are committed.<br><br>
    *
    * This method is intended to be used in cases that require at-least-once-delivery, where
    * messages have to be processed before offsets are committed. By relying on the methods like
    * [[partitionedStream]], [[records]], and similar, you have to correctly implement not only your
    * processing logic but also the correct mechanism for committing offsets. This can be tricky to
    * do in a correct and efficient way.<br><br>
    *
    * Working with `Chunk` s of records has several benefits:<br>
    *   - As a user, you don't have to care about committing offsets correctly. You can focus on
    *     implementing your business logic<br>
    *   - It's very straightforward to batch several messages from a `Chunk` together, e.g. for
    *     efficient writes to a persistent storage<br>
    *   - You can liberally use logic that involves concurrency, filtering, and re-ordering of
    *     messages without having to worry about incorrect offset commits<br>
    *
    * <br>
    *
    * The `processor` is a function that takes a `Chunk[ConsumerRecord[K, V]]` and returns a
    * `F[CommitNow]`. [[CommitNow]] is isomorphic to `Unit`, but helps in transporting the intention
    * that processing of a `Chunk` is done, offsets should be committed, and no important processing
    * should be done afterwards.<br><br>
    *
    * The returned value has the type `F[Nothing]`, because it's a never-ending process that doesn't
    * terminate, and therefore doesn't return a result.
    *
    * @note
    *   This method does not make any use of Kafka's auto-commit feature, it implements "manual"
    *   commits in a way that suits most of the common use cases.
    * @note
    *   you have to first use `subscribe` or `assign` the consumer before using this `Stream`. If
    *   you forgot to subscribe, there will be a [[NotSubscribedException]] raised in the `Stream`.
    * @see
    *   [[partitionedStream]]
    * @see
    *   [[CommitNow]]
    */
  final def consumeChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
  )(implicit F: Concurrent[F]): F[Nothing] = partitionedStream
    .map(
      _.chunks.evalMap(consume(processor))
    )
    .parJoinUnbounded
    .drain
    .compile
    .onlyOrError

  private def consume(processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow])(
    chunk: Chunk[CommittableConsumerRecord[F, K, V]]
  )(implicit F: Monad[F]): F[Unit] = {
    val (offsets, records) =
      chunk.mapAccumulate(CommittableOffsetBatch.empty)((offsetBatch, committableRecord) =>
        (offsetBatch.updated(committableRecord.offset), committableRecord.record)
      )

    processor(records) >> offsets.commit
  }

}

object KafkaConsumeChunk {

  type CommitNow = CommitNow.type

  /**
    * Token to indicate that a `Chunk` has been processed and the corresponding offsets are ready to
    * be committed.<br>
    *
    * Isomorphic to `Unit`, but more intention revealing.
    */
  object CommitNow

}
