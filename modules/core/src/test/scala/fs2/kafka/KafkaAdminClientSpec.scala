/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{IO, SyncIO}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry, NewPartitions, NewTopic}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.{
  PatternType,
  ResourcePattern,
  ResourcePatternFilter,
  ResourceType
}
import org.apache.kafka.common.TopicPartition

final class KafkaAdminClientSpec extends BaseKafkaSpec {

  describe("KafkaAdminClient") {
    it("should allow creating instances") {
      KafkaAdminClient.resource[IO](adminClientSettings).use(IO.pure).unsafeRunSync()
      KafkaAdminClient.stream[IO](adminClientSettings).compile.lastOrError.unsafeRunSync()
      KafkaAdminClient.resourceIn[SyncIO, IO](adminClientSettings).use(SyncIO.pure).unsafeRunSync()
      KafkaAdminClient.streamIn[SyncIO, IO](adminClientSettings).compile.lastOrError.unsafeRunSync()
    }

    it("should support consumer groups-related functionalities") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              consumerGroupIds <- adminClient.listConsumerGroups.groupIds
              consumerGroupId <- IO(consumerGroupIds match {
                                   case List(groupId) => groupId
                                   case _             => fail()
                                 })
              consumerGroupListings   <- adminClient.listConsumerGroups.listings
              _                       <- IO(assert(consumerGroupListings.size == 1))
              describedConsumerGroups <- adminClient.describeConsumerGroups(consumerGroupIds)
              _                       <- IO(assert(describedConsumerGroups.size == 1))
              _ <- IO {
                     adminClient.listConsumerGroups.toString should
                       startWith("ListConsumerGroups$")
                   }
              consumerGroupsOffsets <- consumerGroupIds.parTraverse { groupId =>
                                         adminClient
                                           .listConsumerGroupOffsets(groupId)
                                           .partitionsToOffsetAndMetadata
                                           .map((groupId, _))
                                       }
              offsets <- IO(consumerGroupsOffsets match {
                           case List(offsets) => offsets
                           case _             => fail()
                         })
              (_, consumerGroupOffsetsMap) = offsets
              _ <- IO(
                     assert(
                       consumerGroupOffsetsMap
                         .map { case (_, offset) =>
                           offset.offset()
                         }
                         .forall(_ > 0)
                     )
                   )
              _ <- IO {
                     adminClient
                       .listConsumerGroupOffsets(consumerGroupId)
                       .toString shouldBe "ListConsumerGroupOffsets(groupId = test-group-id)"
                   }
              consumerGroupOffsetsPartitions <- consumerGroupIds.parTraverse { groupId =>
                                                  adminClient
                                                    .listConsumerGroupOffsets(groupId)
                                                    .forPartitions(List.empty[TopicPartition])
                                                    .partitionsToOffsetAndMetadata
                                                    .map((groupId, _))
                                                }
              _ <- IO(assert(consumerGroupOffsetsPartitions.size == 1))
              _ <-
                IO {
                  adminClient
                    .listConsumerGroupOffsets(consumerGroupId)
                    .forPartitions(List(new TopicPartition("topic", 0)))
                    .toString shouldBe "ListConsumerGroupOffsetsForPartitions(groupId = test-group-id, partitions = List(topic-0))"
                }
              partition0    = new TopicPartition(topic, 0)
              updatedOffset = new OffsetAndMetadata(0)
              _ <- adminClient.alterConsumerGroupOffsets(
                     consumerGroupId,
                     Map(partition0 -> updatedOffset)
                   )
              _ <- adminClient
                     .listConsumerGroupOffsets(consumerGroupId)
                     .partitionsToOffsetAndMetadata
                     .map { res =>
                       val expected = consumerGroupOffsetsMap.updated(partition0, updatedOffset)
                       assert {
                         res(partition0) != consumerGroupOffsetsMap(partition0) && res == expected
                       }
                     }
              _ <- adminClient.deleteConsumerGroupOffsets(consumerGroupId, Set(partition0))
              _ <- adminClient
                     .listConsumerGroupOffsets(consumerGroupId)
                     .partitionsToOffsetAndMetadata
                     .map { res =>
                       val expected = consumerGroupOffsetsMap - partition0
                       assert(res == expected)
                     }
              _ <- adminClient.deleteConsumerGroups(consumerGroupIds)
              _ <- adminClient
                     .listConsumerGroups
                     .groupIds
                     .map { res =>
                       val expected = List.empty
                       assert(res == expected)
                     }
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support cluster-related functionalities") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              clusterNodes      <- adminClient.describeCluster.nodes
              _                 <- IO(assert(clusterNodes.size == 1))
              clusterController <- adminClient.describeCluster.controller
              _                 <- IO(assert(!clusterController.isEmpty))
              clusterId         <- adminClient.describeCluster.clusterId
              _                 <- IO(assert(clusterId.nonEmpty))
              _ <- IO {
                     adminClient.describeCluster.toString should startWith("DescribeCluster$")
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support config-related functionalities") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              cr <- IO.pure(new ConfigResource(ConfigResource.Type.TOPIC, topic))
              ce  = new ConfigEntry("cleanup.policy", "delete")
              alteredConfigs <- adminClient
                                  .alterConfigs {
                                    Map(cr -> List(new AlterConfigOp(ce, AlterConfigOp.OpType.SET)))
                                  }
                                  .attempt
              _                <- IO(assert(alteredConfigs.isRight))
              describedConfigs <- adminClient.describeConfigs(List(cr))
              _ <- IO(
                     assert(
                       describedConfigs(cr).exists(actual =>
                         actual.name == ce.name && actual.value == ce.value
                       )
                     )
                   )
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support topic-related functionalities") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              topicNames                   <- adminClient.listTopics.names
              topicCount                    = topicNames.size
              topicListings                <- adminClient.listTopics.listings
              _                            <- IO(assert(topicListings.size == topicCount))
              topicNamesToListings         <- adminClient.listTopics.namesToListings
              _                            <- IO(assert(topicNamesToListings.size == topicCount))
              topicNamesInternal           <- adminClient.listTopics.includeInternal.names
              _                            <- IO(assert(topicNamesInternal.size == topicCount + 1))
              topicListingsInternal        <- adminClient.listTopics.includeInternal.listings
              _                            <- IO(assert(topicListingsInternal.size == topicCount + 1))
              topicNamesToListingsInternal <- adminClient.listTopics.includeInternal.namesToListings
              _                            <- IO(assert(topicNamesToListingsInternal.size == topicCount + 1))
              _ <- IO {
                     adminClient.listTopics.toString should startWith("ListTopics$")
                   }
              _ <- IO {
                     adminClient.listTopics.includeInternal.toString should
                       startWith("ListTopicsIncludeInternal$")
                   }
              describedTopics <- adminClient.describeTopics(topicNames.toList)
              _               <- IO(assert(describedTopics.size == topicCount))
              newTopic         = new NewTopic("new-test-topic", 1, 1.toShort)
              preCreateNames  <- adminClient.listTopics.names
              _               <- IO(assert(!preCreateNames.contains(newTopic.name)))
              _               <- adminClient.createTopic(newTopic)
              postCreateNames <- adminClient.listTopics.names
              createAgain     <- adminClient.createTopics(List(newTopic)).attempt
              _               <- IO(assert(createAgain.isLeft))
              _               <- IO(assert(postCreateNames.contains(newTopic.name)))
              createPartitions <-
                adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(4))).attempt
              _               <- IO(assert(createPartitions.isRight))
              describedTopics <- adminClient.describeTopics(topic :: Nil)
              _               <- IO(assert(describedTopics.size == 1))
              _ <- IO(
                     assert(describedTopics.headOption.exists(_._2.partitions.size == 4))
                   )
              deleteTopics    <- adminClient.deleteTopics(List(topic)).attempt
              _               <- IO(assert(deleteTopics.isRight))
              describedTopics <- adminClient.describeTopics(topic :: Nil).attempt
              _ <- IO(
                     assert(
                       describedTopics.leftMap(_.getMessage()) == Left(
                         "This server does not host this topic-partition."
                       )
                     )
                   )
              deleteTopic    <- adminClient.deleteTopic(newTopic.name()).attempt
              _              <- IO(assert(deleteTopic.isRight))
              describedTopic <- adminClient.describeTopics(newTopic.name() :: Nil).attempt
              _ <- IO(
                     assert(
                       describedTopic.leftMap(_.getMessage()) == Left(
                         "This server does not host this topic-partition."
                       )
                     )
                   )
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support ACLs-related functionality") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              describedAcls <- adminClient.describeAcls(AclBindingFilter.ANY)
              _             <- IO(assert(describedAcls.isEmpty))

              aclEntry = new AccessControlEntry(
                           "User:ANONYMOUS",
                           "*",
                           AclOperation.DESCRIBE,
                           AclPermissionType.ALLOW
                         )
              pattern    = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
              acl        = new AclBinding(pattern, aclEntry)
              _         <- adminClient.createAcls(List(acl))
              foundAcls <- adminClient.describeAcls(AclBindingFilter.ANY)
              _         <- IO(assert(foundAcls.length == 1))
              _         <- IO(assert(foundAcls.head.pattern() === pattern))

              // delete another Entry
              _ <- adminClient.deleteAcls(
                     List(
                       new AclBindingFilter(
                         ResourcePatternFilter.ANY,
                         new AccessControlEntryFilter(
                           "User:ANONYMOUS",
                           "*",
                           AclOperation.WRITE,
                           AclPermissionType.ALLOW
                         )
                       )
                     )
                   )
              foundAcls <- adminClient.describeAcls(AclBindingFilter.ANY)
              _         <- IO(assert(foundAcls.length == 1))

              _         <- adminClient.deleteAcls(List(AclBindingFilter.ANY))
              foundAcls <- adminClient.describeAcls(AclBindingFilter.ANY)
              _         <- IO(assert(foundAcls.isEmpty))
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support misc defined functionality") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              _ <- IO {
                     adminClient.toString should startWith("KafkaAdminClient$")
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }
  }

  def commonSetup(topic: String): Unit = {
    createCustomTopic(topic, partitions = 3)
    val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
    publishToKafka(topic, produced)

    KafkaConsumer
      .stream(consumerSettings[IO])
      .evalTap(_.subscribe(topic.r))
      .records
      .take(produced.size.toLong)
      .map(_.offset)
      .chunks
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
      .compile
      .lastOrError
      .unsafeRunSync()
  }

}
