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
import scala.collection.immutable.SortedSet

final class KafkaShareConsumersSpec extends ShareConsumerBaseKafkaSpec {

  describe("KafkaShareConsumers") {
    it("should settle every record once across consumers with suffixed client ids") {
      withTopic { topic =>
        createTopic(topic, partitions = 3)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(5)
        val (clientIds, seen) = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              KafkaShareConsumers
                .resource(settings.withClientId("app"), 3)
                .use { consumers =>
                  consumers.subscribeTo(topic) *>
                    consumers.consume(
                      _.parTraverseN(8) { r =>
                        r.accept *> seen
                          .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                          .flatMap(s => consumers.stopConsuming.whenA(s.size >= produced.size))
                      }.void
                    ) *> consumers.metrics.map(_.keySet).product(seen.get)
                }
            }
        )
        clientIds shouldBe Set("app-0", "app-1", "app-2")
        seen.map(_._1) should contain theSameElementsAs produced
        all(seen.map(_._2)) shouldBe 1
        assertSettled(topic, settings)
      }
    }

    it("should keep the client id of a single consumer") {
      val clientIds = run(
        KafkaShareConsumers
          .resource(shareConsumerSettings.withClientId("app"), 1)
          .use(consumers =>
            consumers.metrics
              .map(_.keySet)
              .tupleLeft(consumers.consumers.head.settings.properties.get("client.id"))
          )
      )
      clientIds shouldBe (Some("app") -> Set("app"))
    }

    it("should give each consumer's metrics by its generated client id") {
      val clientIds =
        run(KafkaShareConsumers.resource(shareConsumerSettings, 2).use(_.metrics.map(_.keySet)))
      clientIds should have size 2
    }

    it("should return the union of the consumers' subscriptions") {
      withTopic { topic =>
        createTopic(topic)
        val subscription = run(
          KafkaShareConsumers
            .resource(shareConsumerSettings, 2)
            .use(consumers => consumers.consumers.last.subscribeTo(topic) *> consumers.subscription)
        )
        subscription shouldBe SortedSet(topic)
      }
    }

    it("should unsubscribe every consumer") {
      withTopic { topic =>
        createTopic(topic)
        val subscriptions = run(
          KafkaShareConsumers
            .resource(shareConsumerSettings, 2)
            .use(consumers =>
              consumers.subscribeTo(topic) *> consumers.unsubscribe *>
                consumers.consumers.traverse(_.subscription)
            )
        )
        subscriptions.toList.distinct shouldBe List(SortedSet.empty[String])
      }
    }

    // One consumer fails once the other holds a record: that one is canceled and its record released.
    it("should cancel the others when one fails, releasing their records") {
      withTopic { topic =>
        createTopic(topic)
        val produced = (0 until 10).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(1)
        val (result, held) = run(
          (Deferred[IO, String], Ref.of[IO, Option[String]](None)).flatMapN { (holding, held) =>
            KafkaShareConsumers
              .resource(settings, 2)
              .use { consumers =>
                consumers.subscribeTo(topic) *>
                  consumers
                    .consume(_.traverse_ { r =>
                      if (r.record.value == "0")
                        holding.get *> IO.raiseError[Unit](new RuntimeException("boom"))
                      // The first other record is held; any later one is released.
                      else
                        holding
                          .complete(r.record.value)
                          .ifM(held.set(Some(r.record.value)) *> IO.never, IO.unit)
                    })
                    .attempt
              }
              .product(held.get)
          }
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
        publish(topic, "after")
        val again = readUntil(topic, settings, "after")
        again.map(_._1) should contain allElementsOf (produced :+ "after")
        again.filter(_._2 >= 2).map(_._1) should contain allElementsOf ("0" +: held.toList)
      }
    }

    it("should fail for a non-positive number of consumers") {
      val result = run(KafkaShareConsumers.resource(shareConsumerSettings, 0).use_.attempt)
      result.swap.toOption.get shouldBe a[IllegalArgumentException]
    }
  }

}
