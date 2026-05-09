/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import scala.util.chaining._

import cats.effect.kernel.{Outcome, Resource}
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import cats.Parallel
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
import fs2.Chunk

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.trace._
import org.typelevel.otel4s.Attributes

/**
  * A tracing handle bound to a specific [[fs2.kafka.KafkaProducer]].
  *
  * This handle keeps the traced producer API explicit instead of mirroring the full operational
  * surface of [[fs2.kafka.KafkaProducer]]. Use [[underlying]] when you need low-level producer
  * operations such as metrics, partition metadata, or the original two-stage `produce` contract.
  *
  * The traced send operations on this handle are intentionally one-shot awaited effects so the
  * emitted `send` spans line up with Kafka send completion more closely.
  */
trait TracedKafkaProducer[F[_], K, V] {

  /**
    * The underlying Kafka producer this tracing handle is bound to.
    */
  def underlying: KafkaProducer[F, K, V]

  /**
    * Injects the current tracing context into the headers of a single record without producing it.
    */
  def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]]

  /**
    * Injects the current tracing context into the headers of all records in a batch without
    * producing them.
    */
  def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]]

  /**
    * Produces a batch and awaits Kafka completion in one effect.
    *
    * This is the preferred traced producer API when you want `send` span timing to track the actual
    * Kafka send rather than the lifetime of a deferred await handle.
    */
  def produceAwaited(records: ProducerRecords[K, V]): F[ProducerResult[K, V]]

  /**
    * Produces a single record and awaits Kafka completion in one effect.
    */
  def produceOneAwaited(record: ProducerRecord[K, V]): F[ProducerResult[K, V]]

  /**
    * Injects tracing headers into all records before delegating to the underlying transactional
    * produce-and-commit flow.
    */
  def produceAndCommitTransactionally(
    records: TransactionalProducerRecords[F, K, V]
  ): F[ProducerResult[K, V]]

  /**
    * Injects tracing headers into all records before delegating to the underlying transactional
    * produce flow.
    */
  def produceTransactionally(
    records: ProducerRecords[K, V]
  ): F[ProducerResult[K, V]]

  /**
    * Preserves tracing behavior when switching serializers.
    */
  def withSerializers[K2, V2](
    keySerializer: KeySerializer[F, K2],
    valueSerializer: ValueSerializer[F, V2]
  ): TracedKafkaProducer[F, K2, V2]

}

object TracedKafkaProducer {

  final private[otel4s] class Impl[F[_]: MonadCancelThrow: Parallel: Tracer, K: KafkaMessageKey, V](
    override val underlying: KafkaProducer[F, K, V],
    config: KafkaTracer.Config
  ) extends TracedKafkaProducer[F, K, V] {

    override def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]] =
      Tracer[F]
        .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
        .flatMap {
          // if the record headers already have propagated span details, we don't need to inject anything
          case Some(_) =>
            MonadCancelThrow[F].pure(record)

          case None =>
            Tracer[F].propagate(record.headers).map(record.withHeaders)
        }

    override def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]] =
      records.traverse(injectHeaders)

    override def produceAwaited(records: ProducerRecords[K, V]): F[ProducerResult[K, V]] =
      produce(underlying, records).flatten

    override def produceOneAwaited(record: ProducerRecord[K, V]): F[ProducerResult[K, V]] =
      produceAwaited(ProducerRecords.one(record))

    override def produceAndCommitTransactionally(
      records: TransactionalProducerRecords[F, K, V]
    ): F[ProducerResult[K, V]] =
      if (records.isEmpty) MonadCancelThrow[F].pure(Chunk.empty)
      else {
        val grouped =
          records.foldLeft(
            Map.empty[
              KafkaCommitter[F],
              (Map[TopicPartition, OffsetAndMetadata], Chunk[ProducerRecord[K, V]])
            ]
          ) { case (acc, recordBatch) =>
            val offset      = recordBatch.offset
            val committer   = offset.committer
            val nextOffsets = acc
              .get(committer)
              .map(_._1)
              .getOrElse(Map.empty)
              .updatedWith(offset.topicPartition) {
                case existing @ Some(current)
                    if current.offset >= offset.offsetAndMetadata.offset =>
                  existing
                case Some(_) | None =>
                  Some(offset.offsetAndMetadata)
              }
            val nextRecords =
              acc.get(committer).map(_._2).getOrElse(Chunk.empty) ++ recordBatch.records

            acc.updated(committer, nextOffsets -> nextRecords)
          }

        underlying
          .transaction
          .surround {
            Chunk
              .from(grouped.toList)
              .parFlatTraverse { case (committer, offsets) =>
                for {
                  metadata <- committer.metadata
                  result   <- produce(underlying, offsets._2).flatten
                  _        <- underlying.sendOffsetsToTransaction(offsets._1, metadata)
                } yield result
              }
          }
      }

    override def produceTransactionally(
      records: ProducerRecords[K, V]
    ): F[ProducerResult[K, V]] =
      if (records.isEmpty) MonadCancelThrow[F].pure(Chunk.empty)
      else underlying.transaction.surround(produce(underlying, records).flatten)

    override def withSerializers[K2, V2](
      keySerializer: KeySerializer[F, K2],
      valueSerializer: ValueSerializer[F, V2]
    ): TracedKafkaProducer[F, K2, V2] =
      new Impl[F, K2, V2](
        underlying.withSerializers(keySerializer, valueSerializer),
        config
      )

    private case class PreparedRecord(
      record: ProducerRecord[K, V],
      usesSendSpanAsCreationContext: Boolean,
      sendLink: Option[(SpanContext, Attributes)],
      releaseCreateSpan: Option[Resource.ExitCase => F[Unit]]
    )

    private case class PreparedBatch(
      records: ProducerRecords[K, V],
      sendKind: SpanKind,
      sendLinks: List[(SpanContext, Attributes)],
      releaseCreateSpans: Resource.ExitCase => F[Unit]
    )

    private def produce(
      producer: KafkaProducer[F, K, V],
      records: ProducerRecords[K, V]
    ): F[F[ProducerResult[K, V]]] =
      if (records.isEmpty) producer.produce(records)
      else {
        prepareBatch(records).flatMap { prepared =>
          val spanContext = Semconv.sendSpanContext(underlying.metadata, prepared.records)
          val spanSetup   = config.sendSpanSetup(spanContext)

          val span = Tracer[F]
            .spanBuilder(spanSetup.spanName)
            .withSpanKind(prepared.sendKind)
            .withFinalizationStrategy(spanSetup.finalizationStrategy)
            .addAttributes(
              Semconv.sendAttributes(spanContext, prepared.records) ++
                config.constAttributes ++
                spanSetup.attributes
            )
            .pipe { builder =>
              prepared
                .sendLinks
                .foldLeft(builder) { case (acc, (ctx, attributes)) =>
                  acc.addLink(ctx, attributes)
                }
            }
            .build

          MonadCancelThrow[F].uncancelable { poll =>
            span
              .resource
              .allocatedCase
              .flatMap { case (res, release) =>
                val outerProduce =
                  if (prepared.sendKind == SpanKind.Producer)
                    prepared
                      .records
                      .traverse(record => injectHeaders(record))
                      .flatMap(producer.produce(_))
                  else
                    producer.produce(prepared.records)

                MonadCancelThrow[F]
                  .guaranteeCase(poll(res.trace(outerProduce))) {
                    case Outcome.Succeeded(_) =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Succeeded)
                    case Outcome.Errored(e) =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Errored(e)) *>
                        release(Resource.ExitCase.Errored(e))
                    case Outcome.Canceled() =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Canceled) *>
                        release(Resource.ExitCase.Canceled)
                  }
                  .map { awaitResult =>
                    MonadCancelThrow[F].guaranteeCase(
                      res
                        .trace(awaitResult)
                        .flatTap { result =>
                          res.span.addAttributes(Semconv.sendResultAttributes(result))
                        }
                    ) {
                      case Outcome.Succeeded(_) => release(Resource.ExitCase.Succeeded)
                      case Outcome.Errored(e)   => release(Resource.ExitCase.Errored(e))
                      case Outcome.Canceled()   => release(Resource.ExitCase.Canceled)
                    }
                  }
              }
          }
        }
      }

    private def prepareRecord(
      record: ProducerRecord[K, V],
      createCreationContext: Boolean
    ): F[PreparedRecord] =
      Tracer[F]
        .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
        .flatMap {
          case Some(ctx) =>
            MonadCancelThrow[F].pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = false,
                sendLink = Some(ctx -> Semconv.sendLinkAttributes(record)),
                releaseCreateSpan = None
              )
            )

          case None if createCreationContext =>
            Tracer[F]
              .spanBuilder(Semconv.createSpanName(record.topic))
              .withSpanKind(SpanKind.Producer)
              .addAttributes(
                Semconv.createAttributes(underlying.metadata, record) ++ config.constAttributes
              )
              .build
              .resource
              .allocatedCase
              .flatMap { case (res, release) =>
                res
                  .trace(injectHeaders(record))
                  .map { injected =>
                    PreparedRecord(
                      record = injected,
                      usesSendSpanAsCreationContext = false,
                      sendLink = Some(res.span.context -> Semconv.sendLinkAttributes(record)),
                      releaseCreateSpan = Some(release)
                    )
                  }
              }

          case None =>
            MonadCancelThrow[F].pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = true,
                sendLink = None,
                releaseCreateSpan = None
              )
            )
        }

    private def prepareBatch(records: ProducerRecords[K, V]): F[PreparedBatch] = {
      val useCreateSpans = records.size > 1

      records
        .toList
        .traverse(prepareRecord(_, useCreateSpans))
        .map { prepared =>
          val sendUsesOwnContext = prepared.forall(_.usesSendSpanAsCreationContext)
          val releaseCreateSpans = (exitCase: Resource.ExitCase) =>
            prepared
              .reverse
              .traverse_(_.releaseCreateSpan.fold(MonadCancelThrow[F].unit)(_(exitCase)))

          PreparedBatch(
            records = ProducerRecords(prepared.map(_.record)),
            sendKind = if (sendUsesOwnContext) SpanKind.Producer else SpanKind.Client,
            sendLinks = prepared.flatMap(_.sendLink),
            releaseCreateSpans = releaseCreateSpans
          )
        }
    }

  }

}
