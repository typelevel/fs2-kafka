/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

final class KafkaShareConsumerSpec extends ShareConsumerBaseKafkaSpec {

  private type Records = Chunk[AcknowledgeableConsumerRecord[IO, String, String]]

  // Consumes the topic with `process` until it stopped the consumer.
  private def consumed(settings: ShareConsumerSettings[IO, String, String], topic: String)(
    process: KafkaShareConsumer[IO, String, String] => Records => IO[Unit]
  ): IO[Unit] =
    KafkaShareConsumer
      .resource(settings)
      .use(consumer => consumer.subscribeTo(topic) *> consumer.consume(process(consumer)))

  // Accepts records in parallel, and stops once `count` were accepted, returning values and
  // delivery counts.
  private def acceptAll(
    settings: ShareConsumerSettings[IO, String, String],
    topic: String,
    count: Int
  )(work: AcknowledgeableConsumerRecord[IO, String, String] => IO[Unit]): Vector[(String, Int)] =
    run(
      Ref
        .of[IO, Vector[(String, Int)]](Vector.empty)
        .flatMap { seen =>
          consumed(settings, topic) { consumer => records =>
            records
              .parTraverseN(8) { r =>
                work(r) *> r.accept *> seen
                  .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                  .flatMap(s => consumer.stopConsuming.whenA(s.size >= count))
              }
              .void
          } *> seen.get
        }
    )

  // Runs `process` on the first poll only, then stops, returning its result.
  private def firstPoll[A](settings: ShareConsumerSettings[IO, String, String], topic: String)(
    process: Records => IO[A]
  ): IO[A] =
    Ref
      .of[IO, Option[A]](None)
      .flatMap { result =>
        consumed(settings, topic) { consumer => records =>
          process(records).flatMap(a => result.set(Some(a))) *> consumer.stopConsuming
        } *> result.get.map(_.get)
      }

  // Handles each record with `handle`, one at a time, and stops once `count` were handled,
  // returning values and delivery counts in the order handled.
  private def handled(
    settings: ShareConsumerSettings[IO, String, String],
    topic: String,
    count: Int
  )(
    handle: AcknowledgeableConsumerRecord[IO, String, String] => IO[Unit]
  ): IO[Vector[(String, Int)]] =
    Ref
      .of[IO, Vector[(String, Int)]](Vector.empty)
      .flatMap { seen =>
        consumed(settings, topic) { consumer => records =>
          records.traverse_ { r =>
            handle(r) *> seen
              .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
              .flatMap(s => consumer.stopConsuming.whenA(s.size >= count))
          }
        } *> seen.get
      }

  private val accepting: AcknowledgeType = AcknowledgeType.Accept

  describe("creating share consumers") {
    it("should consume with a consumer created as a stream") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .stream(settings)
            .evalMap(consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_.traverse_(_.accept) *> consumer.stopConsuming)
            )
            .compile
            .drain
        )
        assertSettled(topic, settings)
      }
    }

    it("should fail for an unknown acknowledgement mode, as Java's consumer does") {
      val result = run(
        KafkaShareConsumer
          .resource(shareConsumerSettings.withProperty("share.acknowledgement.mode", "manual"))
          .use_
          .attempt
      )
      result.swap.toOption.get shouldBe a[ConfigException]
    }

    it("should accept share.acquire.mode in any case, as Kafka does") {
      run(
        KafkaShareConsumer
          .resource(shareConsumerSettings.withProperty("share.acquire.mode", "RECORD_LIMIT"))
          .use_
      )
    }
  }

  describe("KafkaShareConsumer#consume") {
    it("should accept records processed in parallel") {
      withTopic { topic =>
        createTopic(topic, partitions = 2)
        val produced = (0 until 50).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(10)
        val seen = acceptAll(settings, topic, 50)(_ => IO.unit)
        seen.map(_._1) should contain theSameElementsAs produced
        assertSettled(topic, settings)
      }
    }

    it("should fail with an error if not subscribed") {
      run(
        KafkaShareConsumer.resource(shareConsumerSettings).use(_.consume(_ => IO.unit).attempt)
      ).left.toOption.get shouldBe a[NotSubscribedException]
    }

    it("should fail in implicit mode") {
      val settings = shareConsumerSettings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit)
      val result = run(KafkaShareConsumer.resource(settings).use(_.consume(_ => IO.unit).attempt))
      result.swap.toOption.get shouldBe an[ImplicitAcknowledgementException]
    }

    it("should release the records process leaves unacknowledged") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(consumed(settings, topic)(consumer => _ => consumer.stopConsuming))
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should reject the records process leaves unacknowledged if configured") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withFailureAcknowledgement(AcknowledgeType.Reject)
        run(consumed(settings, topic)(consumer => _ => consumer.stopConsuming))
        assertSettled(topic, settings)
      }
    }

    // No poll follows a failed process, so the releases are sent at once: the record is delivered
    // again well before its lock ends, while the consumer stays open.
    it("should send the releases at once when process fails") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val (result, (elapsed, again)) = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                (
                  consumer.consume(_ => IO.raiseError[Unit](new RuntimeException("boom"))).attempt,
                  IO.blocking(readUntil(topic, settings, "0")).timed
                ).tupled
            }
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
        elapsed should be < lock / 2
        again shouldBe Vector("0" -> 2)
      }
    }

    it("should release the poll when a record fails to deserialize") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "bad")
        val test = for {
          failed <- Ref.of[IO, Boolean](false)
          counts <- Ref.of[IO, Vector[Int]](Vector.empty)
          // Fails the first time only, so that the record can be consumed again.
          values = Deserializer.lift[IO, String] { bytes =>
            failed
              .getAndSet(true)
              .ifM(IO.pure(new String(bytes)), IO.raiseError(new RuntimeException("failed")))
          }
          settings = ShareConsumerSettings(Deserializer[IO, String], values)
            .withProperties(shareConsumerSettings.properties)
          results <- KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              val once = consumer.consume { records =>
                counts.update(_ ++ records.toVector.map(_.offset.deliveryCount.toInt)) *>
                  records.traverse_(_.accept) *> consumer.stopConsuming
              }
              consumer.subscribeTo(topic) *> (once.attempt, once.attempt).tupled
            }
          delivered <- counts.get
        } yield (results, delivered)
        val ((first, second), delivered) = run(test)
        first.left.toOption.map(_.getMessage) shouldBe Some("failed")
        second shouldBe Right(())
        delivered shouldBe Vector(2)
      }
    }

    // Closing the consumer would release the record anyway, so only `Reject` shows which
    // acknowledgement a cancel sends.
    it("should release, not reject, the records when canceled") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withFailureAcknowledgement(AcknowledgeType.Reject)
        run(
          Deferred[IO, Unit].flatMap { received =>
            consumed(settings, topic)(_ => _ => received.complete(()) *> IO.never[Unit])
              .race(received.get)
              .void
          }
        )
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should fail an acknowledgement made after process returned") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val result = run(
          Ref
            .of[IO, Option[AcknowledgeableConsumerRecord[IO, String, String]]](None)
            .flatMap { kept =>
              KafkaShareConsumer
                .resource(shareConsumerSettings)
                .use(consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.consume(records => kept.set(records.head) *> consumer.stopConsuming) *>
                    kept.get.flatMap(_.get.accept.attempt)
                )
            }
        )
        result.left.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should not deliver accepted or rejected records again") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings
        run(handled(settings, topic, 2)(r => if (r.record.value == "0") r.accept else r.reject))
        assertSettled(topic, settings)
      }
    }

    it("should deliver released records again with a higher delivery count") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val deliveries = run(
          handled(shareConsumerSettings, topic, 2)(r =>
            if (r.offset.deliveryCount == 1) r.release else r.accept
          )
        )
        deliveries shouldBe Vector("0" -> 1, "0" -> 2)
      }
    }

    it("should let two consumers process records of the same partition concurrently") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(1)
        val consumed = run(
          (Deferred[IO, Unit], Ref.of[IO, Int](0)).flatMapN { (bothStarted, started) =>
            val consume = firstPoll(settings, topic)(_.traverse { r =>
              started.updateAndGet(_ + 1).flatMap(n => bothStarted.complete(()).whenA(n == 2)) *>
                bothStarted.get *> r.accept.as(r.record.value)
            })
            IO.both(consume, consume)
          }
        )
        (consumed._1 ++ consumed._2).toList.toSet shouldBe Set("0", "1")
      }
    }

    it("should settle every record once with batch_optimized acquisition") {
      withTopic { topic =>
        createTopic(topic)
        val produced = (0 until 20).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings
          .withProperty("share.acquire.mode", "batch_optimized")
          .withMaxPollRecords(2)
        val consumed = run(handled(settings, topic, produced.size)(_.accept))
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }
  }

  describe("KafkaShareConsumer#autoConsume") {
    it("should accept each poll") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings
        val seen = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { seen =>
              KafkaShareConsumer
                .resource(settings.withProperty("share.acknowledgement.mode", "implicit"))
                .use { consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.autoConsume(records =>
                      seen
                        .updateAndGet(_ ++ records.toVector.map(_.value))
                        .flatMap(s => consumer.stopConsuming.whenA(s.size >= 3))
                    ) *> seen.get
                }
            }
        )
        seen should contain theSameElementsAs Vector("0", "1", "2")
        assertSettled(topic, settings)
      }
    }

    it("should not accept a poll when process fails") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result = run(
          KafkaShareConsumer
            .resource(settings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit))
            .use(consumer =>
              consumer.subscribeTo(topic) *>
                consumer.autoConsume(_ => IO.raiseError[Unit](new RuntimeException("boom"))).attempt
            )
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    // Java would accept the failed poll with its next one.
    it("should not consume again once process failed") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val again = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit))
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.autoConsume(_ => IO.raiseError[Unit](new RuntimeException("boom"))).attempt *>
                consumer.autoConsume(_ => IO.unit).attempt
            }
        )
        again.left.toOption.get shouldBe an[IllegalStateException]
      }
    }

    it("should fail in explicit mode") {
      val result =
        run(KafkaShareConsumer.resource(shareConsumerSettings).use(_.autoConsume(_ => IO.unit).attempt))
      result.swap.toOption.get shouldBe an[IllegalStateException]
    }
  }

  describe("KafkaShareConsumer#stopConsuming") {
    it("should send the last acknowledgements before consume returns") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          consumed(settings, topic)(consumer => _.traverse_(r => consumer.stopConsuming *> r.accept))
        )
        assertSettled(topic, settings)
      }
    }

    it("should not consume again once stopped") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val again = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_.traverse_(_.accept) *> consumer.stopConsuming) *>
                consumer.consume(_ => IO.unit).attempt
            }
        )
        again.left.toOption.map(_.getMessage) shouldBe Some("the consumer was stopped")
      }
    }

    it("should finish after an unsubscribe while a renewal is pending") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        run(
          consumed(shareConsumerSettings.withMaxPollRecords(2), topic) { consumer => records =>
            records
              .parTraverseN(2) { r =>
                if (r.record.value == "0") r.renew
                else IO.sleep(1.second) *> consumer.unsubscribe *> r.accept.attempt.void
              }
              .void *> consumer.stopConsuming
          }.timeout(30.seconds)
        )
      }
    }
  }

  describe("KafkaShareConsumer#unsubscribe") {
    it("should consume after unsubscribing and subscribing again") {
      withTopic { first =>
        withTopic { second =>
          createTopic(first)
          createTopic(second)
          publish(first, "first")
          publish(second, "second")
          val consumed = run(
            Ref
              .of[IO, Vector[String]](Vector.empty)
              .flatMap { seen =>
                KafkaShareConsumer
                  .resource(shareConsumerSettings)
                  .use { consumer =>
                    consumer.subscribeTo(first) *>
                      consumer.consume(_.traverse_ { r =>
                        r.accept *>
                          (consumer.unsubscribe *> consumer.subscribeTo(second)).whenA(
                            r.record.value == "first"
                          ) *>
                          seen
                            .updateAndGet(_ :+ r.record.value)
                            .flatMap(s => consumer.stopConsuming.whenA(s.size >= 2))
                      }) *> seen.get
                  }
              }
          )
          consumed shouldBe Vector("first", "second")
        }
      }
    }

    // Java's unsubscribe doesn't send what is pending, so the record would be delivered again.
    // The consumer idles unsubscribed past the lock while the record is read again, so no final
    // commit helps.
    it("should send the pending acknowledgements before unsubscribing") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          Deferred[IO, Unit].flatMap { returned =>
            KafkaShareConsumer
              .resource(settings)
              .use { consumer =>
                consumer.subscribeTo(topic) *>
                  consumer
                    .consume(
                      _.traverse_(_.accept) *> consumer.unsubscribe *> returned.complete(()).void
                    )
                    .background
                    .use(done =>
                      returned.get *> IO.sleep(lock + 2.seconds) *> IO.blocking(
                        assertSettled(topic, settings)
                      ) *> consumer.stopConsuming *> done.flatMap(_.embedNever)
                    )
              }
          }
        )
      }
    }
  }

  describe("KafkaShareConsumer#subscription") {
    it("should return the subscribed topics") {
      withTopic { topic =>
        createTopic(topic)
        val subscription = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use(consumer => consumer.subscribeTo(topic) *> consumer.subscription)
        )
        subscription shouldBe SortedSet(topic)
      }
    }
  }

  describe("KafkaShareConsumer#metrics") {
    it("should return metrics") {
      val metrics = run(KafkaShareConsumer.resource(shareConsumerSettings).use(_.metrics))
      metrics should not be empty
    }
  }

  describe("acknowledging records") {
    // The second record stays unacknowledged past its lock, and no poll runs while process does,
    // so only the delay sends the first's acknowledgement in time.
    it("should send pending acknowledgements after the max acknowledgement delay") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        // The second record is released past its lock, which the broker rejects when stopping.
        run(
          firstPoll(settings.withMaxAcknowledgementDelay(100.millis), topic) { records =>
            records.head.get.accept *> IO.sleep(lock + 1.second) *>
              records.drop(1).traverse_(_.release.attempt)
          }.attempt
        )
        publish(topic, "after")
        readUntil(topic, settings, "after").map(_._1) shouldBe Vector("1", "after")
      }
    }

    it("should send pending acknowledgements with the next poll") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, (0 until 10).map(_.toString)*)
        val settings = shareConsumerSettings.withMaxPollRecords(5).withoutMaxAcknowledgementDelay
        run(
          Ref
            .of[IO, Int](0)
            .flatMap { count =>
              consumed(settings, topic) { consumer => records =>
                Stream
                  .chunk(records)
                  .map(_.offset -> accepting)
                  .through(acknowledgeChunks[IO])
                  .compile
                  .drain *> count
                  .updateAndGet(_ + records.size)
                  .flatMap(n => consumer.stopConsuming.whenA(n >= 10))
              }
            }
        )
        assertSettled(topic, settings)
      }
    }

    it("should keep the last of several acknowledgements for a record") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          firstPoll(settings, topic) { records =>
            Stream
              .chunk(records)
              .flatMap(r =>
                Stream(
                  r.offset -> accepting,
                  r.offset -> (AcknowledgeType.Release: AcknowledgeType)
                )
              )
              .through(acknowledgeChunks[IO])
              .compile
              .drain
          }
        )
        val again = readUntil(topic, settings, "0")
        again.map(_._1) shouldBe Vector("0")
        again.head._2 should be >= 2
      }
    }

    it("should fail an acknowledgement for a record whose acknowledgement was sent") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        // Without a delay, the acknowledgement is sent at once.
        val late = run(
          firstPoll(shareConsumerSettings.withMaxAcknowledgementDelay(Duration.Zero), topic)(
            _.traverse(r => r.accept *> IO.sleep(1.second) *> r.reject.attempt).map(_.head.get)
          )
        )
        late.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should fail an acknowledgement through the offset of an earlier delivery") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val test = for {
          earlier <- Ref.of[IO, Option[AcknowledgeableOffset[IO]]](None)
          result <- Deferred[IO, Either[Throwable, Unit]]
          _ <- consumed(shareConsumerSettings, topic) { consumer =>
            _.traverse_ { r =>
              earlier.get.flatMap {
                // The first delivery is released, and its offset used again once the second arrives.
                case None => earlier.set(Some(r.offset)) *> r.release
                case Some(first) =>
                  first.accept.attempt.flatMap(result.complete) *> r.accept *> consumer.stopConsuming
              }
            }
          }
          stale <- result.get
        } yield stale
        run(test).swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }
  }

  describe("acknowledgeChunks") {
    it("should keep the held delivery's acknowledgement over a later one of an earlier delivery") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          Ref
            .of[IO, Vector[AcknowledgeableOffset[IO]]](Vector.empty)
            .flatMap { offsets =>
              consumed(settings, topic) { consumer =>
                _.traverse_ { r =>
                  offsets
                    .updateAndGet(_ :+ r.offset)
                    .flatMap { seen =>
                      if (seen.size == 1) r.release
                      else
                        Stream
                          .chunk(Chunk(r.offset -> accepting, seen.head -> accepting))
                          .through(acknowledgeChunks[IO])
                          .compile
                          .drain *> consumer.stopConsuming
                    }
                }
              }
            }
        )
        assertSettled(topic, settings)
      }
    }

    it("should settle every record once") {
      withTopic { topic =>
        createTopic(topic, partitions = 2)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              this.consumed(shareConsumerSettings.withMaxPollRecords(5), topic) { consumer => records =>
                Stream
                  .chunk(records)
                  .covary[IO]
                  .parEvalMapUnordered(4) { r =>
                    seen
                      .update(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                      .as(r.offset -> accepting)
                  }
                  .through(acknowledgeChunks[IO])
                  .compile
                  .drain *> seen.get
                  .flatMap(s => consumer.stopConsuming.whenA(s.size >= produced.size))
              } *> seen.get
            }
        )
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }

    it("should send a chunk's acknowledgements to each of its consumers") {
      withTopic { topic =>
        val other = topic + "-other"
        createTopic(topic)
        createTopic(other)
        publish(topic, "a")
        publish(other, "b")
        val settings = shareConsumerSettings
        run(
          (Deferred[IO, Records], Deferred[IO, Records], Deferred[IO, Unit]).flatMapN {
            (fromFirst, fromSecond, acknowledged) =>
              // Each consumer hands its poll out and keeps processing it until both are acknowledged.
              def holding(t: String, out: Deferred[IO, Records]) =
                firstPoll(settings, t)(records => out.complete(records) *> acknowledged.get)
              val acknowledgeBoth =
                (fromFirst.get, fromSecond.get).flatMapN { (a, b) =>
                  Stream
                    .chunk(a ++ b)
                    .map(_.offset -> accepting)
                    .through(acknowledgeChunks[IO])
                    .compile
                    .drain *> acknowledged.complete(()).void
                }
              (holding(topic, fromFirst), holding(other, fromSecond), acknowledgeBoth).parTupled.void
          }
        )
        publish(topic, "after")
        publish(other, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
        readUntil(other, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
  }

  describe("renewal") {
    it("should send an acknowledgement made during a renewal when process fails") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume { records =>
                  // Accepts while the renewal is under way, and fails before it ends.
                  records.traverse_(r => r.renew *> IO.sleep(5.millis) *> r.accept) *>
                    IO.raiseError[Unit](new RuntimeException("boom"))
                }.attempt
            }
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
        assertSettled(topic, settings)
      }
    }

    it("should renew a record automatically while it is decoded past its lock") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val base = shareConsumerSettings
        val values =
          Deserializer.lift[IO, String](bytes => IO.sleep(lock + 1.second).as(new String(bytes)))
        val settings = ShareConsumerSettings(Deserializer[IO, String], values)
          .withProperties(base.properties)
          .withAutoRenewal(true)
        acceptAll(settings, topic, 1)(_ => IO.unit) shouldBe Vector("0" -> 1)
        assertSettled(topic, base)
      }
    }

    it("should accept the rest of a poll while one record outlives its lock") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1", "2", "3")
        val settings = shareConsumerSettings.withMaxPollRecords(4).withAutoRenewal(true)
        val acceptedBefore = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { accepted =>
              firstPoll(settings, topic)(
                _.parTraverseN(4) { r =>
                  if (r.record.value == "0")
                    IO.sleep(lock + 1.second) *> accepted.get.flatTap(_ => r.accept)
                  else
                    r.accept *> accepted.update(_ :+ r.record.value).as(Vector.empty[String])
                }
                  .map(_.toVector.flatten)
              )
            }
        )
        acceptedBefore should contain theSameElementsAs Vector("1", "2", "3")
        assertSettled(topic, settings)
      }
    }

    // The renewal is due again before it returns, which must not keep acknowledgements out.
    it("should let acknowledgements in while each renewal outlasts the renewal interval") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withAutoRenewal(true).withRenewalMargin(lock - 1.nanosecond)
        run(
          consumed(settings, topic)(consumer =>
            _.traverse_(r => IO.sleep(2.seconds) *> r.accept.timeout(20.seconds)) *> consumer.stopConsuming
          )
        )
        assertSettled(topic, settings)
      }
    }

    it("should keep a record past its lock with manual renewals only") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          firstPoll(settings.withAutoRenewal(false), topic)(
            _.traverse_(r => (IO.sleep(lock / 3) *> r.renew).replicateA_(4) *> r.accept)
          )
        )
        assertSettled(topic, settings)
      }
    }

    // The consumer does not track lock expiry itself: the acknowledgement is accepted, and the broker
    // rejects it once sent.
    it("should lose a record past its lock without renewal") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        val (stopped, late) = run(
          Ref
            .of[IO, Chunk[Either[Throwable, Unit]]](Chunk.empty)
            .flatMap { late =>
              firstPoll(settings.withAutoRenewal(false), topic)(records =>
                IO.sleep(lock + 1.second) *> records.traverse(_.accept.attempt).flatMap(late.set)
              ).attempt.flatMap(stopped => late.get.tupleLeft(stopped))
            }
        )
        late.toList.distinct shouldBe List(Right(()))
        stopped.left.toOption.get shouldBe a[KafkaException]
        all(readUntil(topic, settings, "1").map(_._2)) should be >= 2
      }
    }
  }

  describe("max processing time") {
    it("should release a record past the max processing time") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withMaxProcessingTime(2.seconds)
        val late = run(
          Ref
            .of[IO, Option[Either[Throwable, Unit]]](None)
            .flatMap { result =>
              consumed(settings, topic) { consumer => records =>
                records.traverse_(r =>
                  IO.sleep(4.seconds) *> r.accept.attempt.flatMap(a => result.set(Some(a)))
                ) *> consumer.stopConsuming
              } *> result.get
            }
        )
        late.flatMap(_.left.toOption).get shouldBe a[RecordNotInFlightException]
        // Java fetches in the background while process runs, so it may take the released record into
        // its buffer and release it when closing: the delivery count is 2 or 3.
        readUntil(topic, settings, "0").map(_._1) shouldBe Vector("0")
      }
    }
  }

  describe("pausing") {
    it("should not poll while paused") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val test = for {
          paused <- SignallingRef[IO, Boolean](true)
          polls <- Ref.of[IO, Int](0)
          counts <- KafkaShareConsumer
            .resource(shareConsumerSettings.withPauseWhen(paused))
            .use { consumer =>
              val consume = consumer.consume(records =>
                polls.update(_ + 1) *> records.traverse_(_.accept) *> consumer.stopConsuming
              )
              consumer.subscribeTo(topic) *> consume.background
                .use { done =>
                  for {
                    _ <- IO.sleep(7.seconds)
                    whilePaused <- polls.get
                    _ <- paused.set(false)
                    _ <- done.flatMap(_.embedNever)
                    after <- polls.get
                  } yield (whilePaused, after)
                }
            }
        } yield counts
        run(test) shouldBe ((0, 1))
      }
    }

    // Renewing must not acquire the records published while paused.
    it("should not acquire new records when renewing while paused") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val test = for {
          paused <- SignallingRef[IO, Boolean](false)
          seen <- Ref.of[IO, Vector[(String, Int)]](Vector.empty)
          settings = shareConsumerSettings.withAutoRenewal(true).withPauseWhen(paused)
          _ <- consumed(settings, topic) { consumer =>
            _.traverse_ { r =>
              // The first record is held through a pause in which two more are published.
              val held =
                paused.set(true) *> IO.blocking(publish(topic, "1", "2")) *>
                  IO.sleep(lock * 3 / 2) *> r.accept *> paused.set(false)
              (if (r.record.value == "0") held else r.accept) *> seen
                .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                .flatMap(s => consumer.stopConsuming.whenA(s.size >= 3))
            }
          }
          result <- seen.get
        } yield result
        run(test) should contain theSameElementsAs Vector("0" -> 1, "1" -> 1, "2" -> 1)
      }
    }

    it("should accept a poll while paused in implicit mode") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withProperty("share.acknowledgement.mode", "implicit")
        run(
          SignallingRef[IO, Boolean](false).flatMap { paused =>
            KafkaShareConsumer
              .resource(settings.withPauseWhen(paused))
              .use { consumer =>
                consumer.subscribeTo(topic) *>
                  consumer
                    .autoConsume(_ => paused.set(true))
                    .background
                    // Past the record's lock, also after joining the group.
                    .surround(IO.sleep(lock + 5.seconds)) *> consumer.stopConsuming
              }
          }
        )
        assertSettled(topic, settings.withProperty("share.acknowledgement.mode", "explicit"))
      }
    }

    it("should fail consume when the pause signal fails") {
      withTopic { topic =>
        createTopic(topic)
        val failing = new Signal[IO, Boolean] {
          override def discrete: Stream[IO, Boolean] = Stream.raiseError[IO](new RuntimeException("boom"))
          override def continuous: Stream[IO, Boolean] = Stream.constant(false)
          override def get: IO[Boolean] = IO.pure(false)
        }
        val result = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withPauseWhen(failing))
            .use(consumer =>
              consumer.subscribeTo(topic) *> consumer.consume(_ => IO.unit).attempt.timeout(20.seconds)
            )
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
      }
    }
  }

}
