/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.annotation.nowarn
import scala.concurrent.Promise

import cats.{Apply, Functor, Parallel}
import cats.effect.*
import cats.effect.std.Semaphore
import cats.effect.syntax.all.*
import cats.effect.Resource.ExitCase
import cats.syntax.all.*
import fs2.*
import fs2.kafka.internal.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.producer.MkProducer

import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

/**
  * [[KafkaProducer]] represents a producer of Kafka records, with the ability to produce
  * `ProducerRecord`s using [[produce]].
  */
abstract class KafkaProducer[F[_], K, V] {

  /**
    * Produces the specified [[ProducerRecords]] in two steps: the first effect puts the records in
    * the buffer of the producer, and the second effect waits for the records to send.<br><br>
    *
    * It's possible to `flatten` the result from this function to have an effect which both sends
    * the records and waits for them to finish sending.<br><br>
    *
    * Waiting for individual records to send can substantially limit performance. In some cases,
    * this is necessary, and so we might want to consider the following alternatives.<br><br>
    *
    *   - Wait for the produced records in batches, improving the rate at which records are
    *     produced, but loosing the guarantee where `produce >> otherAction` means `otherAction`
    *     executes after the record has been sent.<br>
    *   - Run several `produce.flatten >> otherAction` concurrently, improving the rate at which
    *     records are produced, and still have `otherAction` execute after records have been sent,
    *     but losing the order of produced records.
    */
  def produce(records: ProducerRecords[K, V]): F[F[ProducerResult[K, V]]]

  /**
    * Enables transactions for the producer.
    *
    * If using [[KafkaProducer.transactional]] or [[KafkaProducer.transactionalStream]], then this
    * will automatically be done when the producer is created. If not, then this function has to be
    * called manually before using any of the transaction methods:
    *
    *   - [[KafkaProducer#transaction]],
    *   - [[KafkaProducer#produceTransactionally]],
    *   - [[KafkaProducer#produceAndCommitTransactionally]]
    *
    * or an `IllegalStateException` exception will be raised.
    */
  def initTransactions: F[Unit]

  /**
    * Return a resource which handles the transaction lifecycle.
    *
    * The returned resource begins a transaction on `use` and:
    *   - commits the transaction if the `use` finishes successfully, or
    *   - aborts the transaction if an error occurs or the process is canceled.
    *
    * Note [[initTransactions]] must have been called before using this method, either manually or
    * automatically through the use of [[KafkaProducer.transactional]] or
    * [[KafkaProducer.transactionalStream]].
    *
    * Also note only one transaction can be open at any time. If a second transaction is started
    * within the lifecycle of a first transaction, the second transaction will deadlock.
    *
    * {{{
    * producer.transaction.surround {
    *   producer.transaction.surround { // deadlocks waiting for the outer transaction
    *     IO.unit
    *   }
    * }
    * }}}
    */
  def transaction: Resource[F, Unit]

  /**
    * Sends the specified offsets and [[KafkaConsumer.groupMetadata]] to be committed as part of a
    * transaction.
    */
  def sendOffsetsToTransaction(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    groupMetadata: ConsumerGroupMetadata
  ): F[Unit]

  /**
    * Produces the `ProducerRecord`s in the specified [[TransactionalProducerRecords]] in four
    * steps: first a transaction is initialized, then the records are placed in the buffer of the
    * producer, then the offsets of the records are sent to the transaction, and lastly the
    * transaction is committed. If errors or cancellation occurs, the transaction is aborted. The
    * returned effect succeeds if the whole transaction completes successfully.
    */
  def produceAndCommitTransactionally(
    records: TransactionalProducerRecords[F, K, V]
  ): F[ProducerResult[K, V]]

  /**
    * Produces the `ProducerRecord`s in the specified [[ProducerRecords]] in three steps: first a
    * transaction is initialized, then the records are placed in the buffer of the producer, and
    * lastly the transaction is committed. If errors or cancellation occurs, the transaction is
    * aborted. The returned effect succeeds if the whole transaction completes successfully.
    */
  def produceTransactionally(records: ProducerRecords[K, V]): F[ProducerResult[K, V]]

  /**
    * Returns producer metrics.
    *
    * @see
    *   org.apache.kafka.clients.producer.KafkaProducer#metrics
    */
  def metrics: F[Map[MetricName, Metric]]

  /**
    * Returns partition metadata for the given topic.
    *
    * @see
    *   org.apache.kafka.clients.producer.KafkaProducer#partitionsFor
    */
  def partitionsFor(topic: String): F[List[PartitionInfo]]

  /**
    * Returns a new [[KafkaProducer]] using the same underlying producer but with different key and
    * value serializers.
    */
  def withSerializers[K2, V2](
    keySerializer: KeySerializer[F, K2],
    valueSerializer: ValueSerializer[F, V2]
  ): KafkaProducer[F, K2, V2]

}

object KafkaProducer {

  implicit class ProducerOps[F[_], K, V](private val producer: KafkaProducer[F, K, V])
      extends AnyVal {

    /**
      * Produce a single [[ProducerRecord]], see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne_(record: ProducerRecord[K, V])(implicit F: Functor[F]): F[F[RecordMetadata]] =
      produceOne(record).map(_.map { res =>
        res.head.get._2 // Should always be present so get is ok
      })

    /**
      * Produce a single record to the specified topic using the provided key and value, see
      * [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne_(topic: String, key: K, value: V)(implicit F: Functor[F]): F[F[RecordMetadata]] =
      produceOne_(ProducerRecord(topic, key, value))

    /**
      * Produce a single record to the specified topic using the provided key and value, see
      * [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne(
      topic: String,
      key: K,
      value: V
    ): F[F[ProducerResult[K, V]]] =
      produceOne(ProducerRecord(topic, key, value))

    /**
      * Produce a single [[ProducerRecord]], see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne(record: ProducerRecord[K, V]): F[F[ProducerResult[K, V]]] =
      producer.produce(ProducerRecords.one(record))

  }

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context, using the specified
    * [[ProducerSettings]]. Note that there is another version where `F[_]` is specified explicitly
    * and the key and value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * KafkaProducer.stream[F].using(settings)
    * }}}
    */
  def stream[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkProducer[F], P: Parallel[F]): Stream[F, KafkaProducer[F, K, V]] =
    Stream.resource(KafkaProducer.resource(settings)(F, mk, P))

  /**
    * Creates a new [[KafkaProducer]] in the `Resource` context, using the specified
    * [[ProducerSettings]]. Note that there is another version where `F[_]` is specified explicitly
    * and the key and value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * KafkaProducer.stream[F].using(settings)
    * }}}
    */
  def transactional[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    P: Parallel[F]
  ): Resource[F, KafkaProducer[F, K, V]] = {
    for {
      producer <- KafkaProducer.resource(settings)(F, mk, P)
      _        <- producer.initTransactions.toResource
    } yield producer
  }

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context, using the specified
    * [[ProducerSettings]]. Note that there is another version where `F[_]` is specified explicitly
    * and the key and value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * KafkaProducer.stream[F].using(settings)
    * }}}
    */
  def transactionalStream[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    P: Parallel[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    Stream.resource(transactional(settings)(F, mk, P))

  /**
    * Creates a [[KafkaProducer]] using the provided settings and produces record in batches.
    */
  def pipe[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    P: Parallel[F]
  ): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    records => stream(settings)(F, mk, P).flatMap(pipe(_).apply(records))

  /**
    * Produces records in batches using the provided [[KafkaProducer]].
    */
  def pipe[F[_]: Concurrent, K, V](
    producer: KafkaProducer[F, K, V]
  ): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    _.evalMap(producer.produce).parEvalMap(Int.MaxValue)(identity)

  /**
    * Creates a new [[KafkaProducer]] in the `Resource` context, using the specified
    * [[ProducerSettings]]. Note that there is another version where `F[_]` is specified explicitly
    * and the key and value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * KafkaProducer.resource[F].using(settings)
    * }}}
    */
  def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    P: Parallel[F]
  ): Resource[F, KafkaProducer[F, K, V]] = {
    for {
      keySerializer   <- settings.keySerializer
      valueSerializer <- settings.valueSerializer
      producer        <- mk(settings).toResource
      semaphore       <- Semaphore(1).toResource
      blocking         =
        settings.customBlockingContext.fold(Blocking.fromSync[F])(Blocking.fromExecutionContext)
      close            = blocking(producer.close(java.time.Duration.ofMillis(settings.closeTimeout.toMillis)))
      _               <- Resource.onFinalize(close)
      fs2KafkaProducer = resourceInternal[F, K, V](
                           settings.failFastProduce,
                           keySerializer,
                           valueSerializer,
                           producer,
                           semaphore,
                           blocking
                         )
    } yield fs2KafkaProducer
  }

  private def resourceInternal[F[_], K, V](
    failFastProduce: Boolean,
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    producer: KafkaByteProducer,
    txSemaphore: Semaphore[F],
    blocking: Blocking[F]
  )(implicit F: Async[F], P: Parallel[F]): KafkaProducer[F, K, V] = new KafkaProducer[F, K, V] {

    override def toString: String =
      "KafkaProducer$" + System.identityHashCode(this)

    def withSerializers[K2, V2](
      k: KeySerializer[F, K2],
      v: ValueSerializer[F, V2]
    ): KafkaProducer[F, K2, V2] =
      resourceInternal[F, K2, V2](failFastProduce, k, v, producer, txSemaphore, blocking)

    override def produce(records: ProducerRecords[K, V]): F[F[ProducerResult[K, V]]] = {
      if (failFastProduce)
        Async[F]
          .delay(Promise[Throwable]())
          .flatMap { produceRecordError =>
            Async[F]
              .race(
                Async[F].fromFutureCancelable(
                  Async[F].delay((produceRecordError.future, Async[F].unit))
                ),
                produceRecords(records, produceRecordError.some)
              )
              .rethrow
          }
      else produceRecords(records, None)
    }

    override def initTransactions: F[Unit] =
      blocking(producer.initTransactions())

    override def transaction: Resource[F, Unit] = {
      val acquireTx = blocking(producer.beginTransaction())
      val commitTx  = blocking(producer.commitTransaction())
      val abortTx   = blocking(producer.abortTransaction())
      for {
        _ <- txSemaphore.permit
        _ <- Resource.makeCase(acquireTx) {
               case (_, ExitCase.Succeeded)  => commitTx
               case (_, ExitCase.Canceled)   => abortTx
               case (_, ExitCase.Errored(e)) => abortTx *> e.raiseError[F, Unit]
             }
      } yield ()
    }

    override def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata
    ): F[Unit] = blocking(producer.sendOffsetsToTransaction(offsets.asJava, groupMetadata))

    override def produceAndCommitTransactionally(
      r: TransactionalProducerRecords[F, K, V]
    ): F[ProducerResult[K, V]] = {
      val records: Chunk[CommittableProducerRecords[F, K, V]] = r
      if (records.isEmpty) F.pure(Chunk.empty)
      else {
        val batch   = CommittableOffsetBatch.fromFoldableMap(records)(_.offset)
        val offsets = Chunk.from(batch.offsets.toVector)
        transaction.use { _ =>
          offsets.parFlatTraverse { case (committer, offsets) =>
            for {
              metadata       <- committer.metadata
              producerRecords = records.filter(_.offset.committer == committer).flatMap(_.records)
              result         <- produce(producerRecords).flatten
              _              <- sendOffsetsToTransaction(offsets, metadata)
            } yield result
          }
        }
      }
    }

    override def produceTransactionally(
      records: ProducerRecords[K, V]
    ): F[ProducerResult[K, V]] =
      if (records.isEmpty) F.pure(Chunk.empty)
      else { transaction.use(_ => produce(records).flatten) }

    /**
      * Returns producer metrics.
      *
      * @see
      *   org.apache.kafka.clients.producer.KafkaProducer#metrics
      */
    override def metrics: F[Map[MetricName, Metric]] =
      blocking(producer.metrics().asScala.toMap[MetricName, Metric])

    /**
      * Returns partition metadata for the given topic.
      *
      * @see
      *   org.apache.kafka.clients.producer.KafkaProducer#partitionsFor
      */
    override def partitionsFor(topic: String): F[List[PartitionInfo]] =
      blocking(producer.partitionsFor(topic).asScala.toList)

    private def produceRecord(
      produceRecordError: Option[Promise[Throwable]]
    )(implicit
      F: Async[F]
    ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
      record =>
        asJavaRecord(keySerializer, valueSerializer, record).flatMap { javaRecord =>
          F.delay(Promise[(ProducerRecord[K, V], RecordMetadata)]())
            .flatMap { promise =>
              blocking {
                producer.send(
                  javaRecord,
                  { (metadata, exception) =>
                    if (exception == null) { promise.success((record, metadata)) }
                    else {
                      promise.failure(exception)
                      produceRecordError.foreach(_.tryFailure(exception))
                    }
                  }
                )
              }.map(javaFuture =>
                F.fromFutureCancelable(
                  F.delay((promise.future, F.delay(javaFuture.cancel(true)).void))
                )
              )
            }
        }

    private def produceRecords(
      records: ProducerRecords[K, V],
      produceRecordError: Option[Promise[Throwable]]
    ): F[F[Chunk[(ProducerRecord[K, V], RecordMetadata)]]] =
      records.traverse(produceRecord(produceRecordError)).map(_.sequence)
  }

  override def toString: String =
    "KafkaProducer$" + System.identityHashCode(this)

  private[this] def serializeToBytes[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[(Array[Byte], Array[Byte])] = {
    val keyBytes =
      keySerializer.serialize(record.topic, record.headers, record.key)

    val valueBytes =
      valueSerializer.serialize(record.topic, record.headers, record.value)

    keyBytes.product(valueBytes)
  }

  private[this] def asJavaRecord[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[KafkaByteProducerRecord] =
    serializeToBytes(keySerializer, valueSerializer, record).map { case (keyBytes, valueBytes) =>
      new KafkaByteProducerRecord(
        record.topic,
        record.partition.fold[java.lang.Integer](null)(identity),
        record.timestamp.fold[java.lang.Long](null)(identity),
        keyBytes,
        valueBytes,
        record.headers.asJava
      )
    }

  def apply[F[_]]: ProducerPartiallyApplied[F] =
    new ProducerPartiallyApplied

  final private[kafka] class ProducerPartiallyApplied[F[_]](val dummy: Boolean = true)
      extends AnyVal {

    /**
      * Alternative version of `resource` where the `F[_]` is specified explicitly, and where the
      * key and value type can be inferred from the [[ProducerSettings]]. This allows you to use the
      * following syntax.
      *
      * {{{
      * KafkaProducer[F].resource(settings)
      * }}}
      */
    def resource[K, V](settings: ProducerSettings[F, K, V])(implicit
      F: Async[F],
      mk: MkProducer[F],
      P: Parallel[F]
    ): Resource[F, KafkaProducer[F, K, V]] =
      KafkaProducer.resource(settings)(F, mk, P)

    /**
      * Alternative version of `stream` where the `F[_]` is specified explicitly, and where the key
      * and value type can be inferred from the [[ProducerSettings]]. This allows you to use the
      * following syntax.
      *
      * {{{
      * KafkaProducer[F].stream(settings)
      * }}}
      */
    def stream[K, V](settings: ProducerSettings[F, K, V])(implicit
      F: Async[F],
      mk: MkProducer[F],
      P: Parallel[F]
    ): Stream[F, KafkaProducer[F, K, V]] =
      KafkaProducer.stream(settings)(F, mk, P)

    override def toString: String =
      "ProducerPartiallyApplied$" + System.identityHashCode(this)

  }

  /*
   * Prevents the default `MkProducer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("msg=never used")
  implicit private def mkAmbig1[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")

  @nowarn("msg=never used")
  implicit private def mkAmbig2[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")

}
