/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.time

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.{IO, SyncIO}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import fs2.{Chunk, Stream}

import org.apache.kafka.clients.admin.{
  AlterConfigOp,
  ConfigEntry,
  FeatureUpdate,
  MemberToRemove,
  NewPartitionReassignment,
  NewPartitions,
  NewTopic,
  OffsetSpec,
  RecordsToDelete
}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{
  ElectionNotNeededException,
  LogDirNotFoundException,
  ProducerFencedException,
  UnsupportedByAuthenticationException
}
import org.apache.kafka.common.quota.{
  ClientQuotaAlteration,
  ClientQuotaEntity,
  ClientQuotaFilter,
  ClientQuotaFilterComponent
}
import org.apache.kafka.common.resource.{
  PatternType,
  ResourcePattern,
  ResourcePatternFilter,
  ResourceType
}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica

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
              consumerGroupId  <- IO(consumerGroupIds match {
                                   case List(groupId) => groupId
                                   case _             => fail()
                                 })
              consumerGroupListings   <- adminClient.listConsumerGroups.listings
              _                       <- IO(assert(consumerGroupListings.size == 1))
              describedConsumerGroups <- adminClient.describeConsumerGroups(consumerGroupIds)
              _                       <- IO(assert(describedConsumerGroups.size == 1))
              _                       <- IO {
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
              _                           <- IO(
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
              _            <- adminClient.alterConsumerGroupOffsets(
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
              _                 <- IO {
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
              cr                    <- IO.pure(new ConfigResource(ConfigResource.Type.TOPIC, topic))
              delete                 = new ConfigEntry("cleanup.policy", "delete")
              compact                = new ConfigEntry("cleanup.policy", "compact")
              invalid                = new ConfigEntry("cleanup.policy", "foo")
              setDelete              = Map(cr -> List(new AlterConfigOp(delete, AlterConfigOp.OpType.SET)))
              setCompact             = Map(cr -> List(new AlterConfigOp(compact, AlterConfigOp.OpType.SET)))
              _                     <- adminClient.alterConfigs[List](setDelete)
              describeAfterSet      <- adminClient.describeConfigs(List(cr))
              _                     <- adminClient.alterConfigs[List](setCompact, validateOnly = true)
              describeAfterValidate <- adminClient.describeConfigs(List(cr))
            } yield {
              describeAfterSet(cr).exists(actual =>
                actual.name == delete.name && actual.value == delete.value
              )
              assert(describeAfterSet.get(cr) == describeAfterValidate.get(cr))
            }
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
              _                            <- IO {
                     adminClient.listTopics.toString should startWith("ListTopics$")
                   }
              _ <- IO {
                     adminClient.listTopics.includeInternal.toString should
                       startWith("ListTopicsIncludeInternal$")
                   }
              describedTopics  <- adminClient.describeTopics(topicNames.toList)
              _                <- IO(assert(describedTopics.size == topicCount))
              newTopic          = new NewTopic("new-test-topic", 1, 1.toShort)
              preCreateNames   <- adminClient.listTopics.names
              _                <- IO(assert(!preCreateNames.contains(newTopic.name)))
              _                <- adminClient.createTopic(newTopic)
              postCreateNames  <- adminClient.listTopics.names
              createAgain      <- adminClient.createTopics(List(newTopic)).attempt
              _                <- IO(assert(createAgain.isLeft))
              _                <- IO(assert(postCreateNames.contains(newTopic.name)))
              createPartitions <-
                adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(4))).attempt
              _               <- IO(assert(createPartitions.isRight))
              describedTopics <- adminClient.describeTopics(topic :: Nil)
              _               <- IO(assert(describedTopics.size == 1))
              _               <- IO(
                     assert(describedTopics.headOption.exists(_._2.partitions.size == 4))
                   )
              deleteTopics    <- adminClient.deleteTopics(List(topic)).attempt
              _               <- IO(assert(deleteTopics.isRight))
              describedTopics <- adminClient.describeTopics(topic :: Nil).attempt
              _               <- IO(
                     assert(
                       describedTopics.leftMap(_.getMessage()) == Left(
                         "This server does not host this topic-partition."
                       )
                     )
                   )
              deleteTopic <- adminClient.deleteTopic(newTopic.name()).attempt
              _           <- IO(assert(deleteTopic.isRight))
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

    it("should support log directory-related functionality") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            for {
              nodes       <- adminClient.describeCluster.nodes
              brokerId    <- IO(nodes.head.id)
              logDirs     <- adminClient.describeLogDirs(List(brokerId))
              _           <- IO(assert(logDirs.nonEmpty))
              _           <- IO(assert(logDirs.get(brokerId).exists(_.nonEmpty)))
              replica      = new TopicPartitionReplica(topic, 0, brokerId)
              replicaDirs <- adminClient.describeReplicaLogDirs(List(replica))
              _           <- IO(assert(replicaDirs.contains(replica)))
              alterDirs   <-
                adminClient.alterReplicaLogDirs(Map(replica -> "/nonexistent-log-dir")).attempt
            } yield {
              alterDirs match {
                case Left(_: LogDirNotFoundException) => succeed
                case other                            =>
                  fail(s"expected alterReplicaLogDirs to fail for invalid log directory but got: $other")
              }
            }
          }
          .unsafeRunSync()
      }
    }

    it("should support listOffsets and deleteRecords") {
      withKafkaConsumer(defaultConsumerProperties) { consumer =>
        withTopic { topic =>
          commonSetup(topic)
          KafkaAdminClient
            .resource[IO](adminClientSettings)
            .use { adminClient =>
              val tp = new TopicPartition(topic, 0)
              for {
                offsets <- adminClient.listOffsets(
                             Map(tp -> OffsetSpec.latest()),
                             IsolationLevel.READ_COMMITTED
                           )
                _            <- IO(assert(offsets(tp).offset() > 0L))
                _             = println(offsets(tp).offset())
                beforeDelete <-
                  IO.blocking(consumer.beginningOffsets(List(tp).asJava)).map(_.asScala)
                _ <- adminClient.deleteRecords(
                       Map(tp -> RecordsToDelete.beforeOffset(offsets(tp).offset()))
                     )
                afterDelete <-
                  IO.blocking(consumer.beginningOffsets(List(tp).asJava)).map(_.asScala)
              } yield assert(beforeDelete(tp) < afterDelete(tp))
            }
            .unsafeRunSync()
        }
      }

    }

    it("should support partition reassignment-related functionality") {
      withTopic { topic =>
        commonSetup(topic)
        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            val tp = new TopicPartition(topic, 0)
            for {
              nodes         <- adminClient.describeCluster.nodes
              brokerId      <- IO(nodes.head.id)
              all           <- adminClient.listPartitionReassignments(None)
              filtered      <- adminClient.listPartitionReassignments(Some(Set(tp)))
              _             <- IO(assert(all == Map.empty))
              _             <- IO(assert(filtered == Map.empty))
              targetReplicas = new NewPartitionReassignment(List(Integer.valueOf(brokerId)).asJava)
              _             <- adminClient.alterPartitionReassignments(Map(tp -> Some(targetReplicas)))
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support quotas, features, SCRAM, metadata quorum, and config resource listing") {
      withTopic { topic =>
        commonSetup(topic)
        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            val qClientId = s"fs2-kafka-quota-$topic"
            val byteRate  = 1048576.0
            // User + client-id matches PLAINTEXT / StandardAuthorizer.
            val quotaMap = Map(
              ClientQuotaEntity.USER      -> "User:ANONYMOUS",
              ClientQuotaEntity.CLIENT_ID -> qClientId
            ).asJava
            val metaFeature     = "metadata.version"
            val quotaEntity     = new ClientQuotaEntity(quotaMap)
            val quotaOp         = new ClientQuotaAlteration.Op("producer_byte_rate", byteRate)
            val quotaAlteration = new ClientQuotaAlteration(quotaEntity, List(quotaOp).asJava)
            val quotaFilter     =
              ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, qClientId)
            val describeFilter = ClientQuotaFilter.contains(List(quotaFilter).asJava)
            for {
              _ <- adminClient.alterClientQuotas(List(quotaAlteration))
              _ <- Stream
                     .awakeEvery[IO](500.millis)
                     .evalMap(_ => adminClient.describeClientQuotas(describeFilter))
                     .map(_.get(quotaEntity).flatMap(_.get("producer_byte_rate")))
                     .collect { case Some(quota) => quota }
                     .filter(_ == byteRate)
                     .take(1)
                     .interruptAfter(30.seconds)
                     .compile
                     .drain
              _       <- adminClient.describeUserScramCredentials(None) // no assertion other than we can make the call
              feature <-
                adminClient.describeFeatures().map(_.supportedFeatures().asScala(metaFeature))
              update     = new FeatureUpdate(feature.maxVersion(), FeatureUpdate.UpgradeType.UPGRADE)
              _         <- adminClient.updateFeatures(Map(metaFeature -> update), true)
              quorum    <- adminClient.describeMetadataQuorum()
              _         <- IO(assert(quorum.leaderId() >= 0))
              resources <- adminClient.listConfigResources()
              _         <- IO(assert(resources.nonEmpty))
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should support delegation token-related functionality") {
      withTopic { topic =>
        commonSetup(topic)

        val renewers  = List(KafkaPrincipal.ANONYMOUS)
        val maxLifeMs = Some(1.hour)
        val badHmac   = Array[Byte](0)
        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            // token renewal mechanics require SSL to be created and managed.
            // To avoid the need for SSL, we will assert that the call fails with an UnsupportedByAuthenticationException meaning it was done
            for {
              createResult <-
                adminClient.createDelegationToken(renewers, owner = None, maxLifeMs).attempt
              renewResult  <- adminClient.renewDelegationToken(badHmac, None).attempt
              expireResult <- adminClient.expireDelegationToken(badHmac, None).attempt
            } yield {
              createResult match {
                case Left(e: UnsupportedByAuthenticationException) => succeed
                case other                                         => fail(s"expected createDelegationToken to fail but got: $other")
              }
              renewResult match {
                case Left(e: UnsupportedByAuthenticationException) => succeed
                case other                                         => fail(s"expected createDelegationToken to fail but got: $other")
              }
              expireResult match {
                case Left(e: UnsupportedByAuthenticationException) => succeed
                case other                                         => fail(s"expected createDelegationToken to fail but got: $other")
              }
            }
          }
          .unsafeRunSync()
      }
    }

    it("should support removeMembersFromConsumerGroup") {
      val groupID    = defaultConsumerProperties(ConsumerConfig.GROUP_ID_CONFIG)
      val instanceID = "my-instance-id"
      withKafkaConsumer(
        defaultConsumerProperties + (ConsumerConfig.GROUP_INSTANCE_ID_CONFIG -> instanceID)
      ) { consumer =>
        withTopic { topic =>
          commonSetup(topic)
          KafkaAdminClient
            .resource[IO](adminClientSettings)
            .use { adminClient =>
              for {
                _            <- IO.blocking(consumer.subscribe(topic.pure[List].asJava))
                _            <- IO.blocking(consumer.poll(time.Duration.ofMillis(100)))
                groupMembers <-
                  adminClient.describeConsumerGroups(List(groupID)).map(_(groupID)).map(_.members())
                toRemove = groupMembers.asScala.map(member => new MemberToRemove(instanceID)).toList
                _       <-
                  adminClient.removeMembersFromConsumerGroup[List](groupID, toRemove, reason = None)
              } yield ()
            }
            .unsafeRunSync()
        }
      }
    }

    it("should support transaction and producer state-related functionality") {
      val transactionID = "some-random-tx-id"
      withTopic { topic =>
        commonSetup(topic)
        val tp = new TopicPartition(topic, 0)
        (for {
          producer <- LowLevelKafkaProducer.resource[IO, Unit, Unit](
                        TransactionalProducerSettings(
                          transactionID,
                          ProducerSettings[IO, Unit, Unit].withProperties(defaultConsumerProperties)
                        )
                      )
          adminClient <- KafkaAdminClient.resource[IO](adminClientSettings)
          _           <- producer.transaction // start transaction so that it can be managed
          _           <- producer             // we are forced to produce in order for the transaction to become visble
                 .produce(
                   Chunk.singleton(ProducerRecord(tp.topic(), (), ()).withPartition(tp.partition()))
                 )
                 .toResource
        } yield (producer, adminClient))
          .use { case (producer, adminClient) =>
            for {
              listed       <- adminClient.listTransactions(None, None, None)
              _            <- IO(assert(listed.exists(_.transactionalId() == transactionID)))
              myTx          = listed.find(_.transactionalId() == transactionID).get
              filteredList <- adminClient.listTransactions(
                                Set(myTx.state()).some,
                                Set(myTx.producerId()).some,
                                0L.some
                              )
              _         <- IO(assert(filteredList.exists(_.transactionalId() == transactionID)))
              described <- adminClient.describeTransactions[List](transactionID.pure[List])
              _         <- IO(assert(described.contains(transactionID)))
              producers <- adminClient.describeProducers(tp.some, none)
              _         <- IO(assert(producers.size == 1))
              _         <- adminClient.fenceProducers[List](myTx.transactionalId().pure[List])
            } yield ()
          }
          .attempt
          .flatMap {
            // the resource will be deallocated.
            // When it does it attempts to commit the transaction but since the producer was fenced it should fail
            // We assert that the failure does happen.
            case Left(_: ProducerFencedException) => succeed.pure[IO]
            case _                                => IO(fail("expected ProducerFencedException when deallocating transaction"))
          }
          .unsafeRunSync()
      }
    }

    it("should support aborting an ongoing transaction") {
      // Transaction cancelation through the admin client is non trivial to test.
      // An initial attempt was done using the list transactions followed by an assertion on the state change but
      // as far as I was able to investigate, the state won't really reflect the aborted state. (it moves to CompleteCommit)

      // A new approach was therefore needed. This test asserts that when the admin client aborts a transaction, the
      // sent records are indeed not committed.

      // We assert that by subsequntly producing records and asserting that the aborted ones are not visible while the ones
      // produced afterwards are.
      val transactionID = "some-random-tx-id"
      withKafkaConsumer(
        defaultConsumerProperties + (ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
      ) { consumer =>
        withTopic { topic =>
          commonSetup(topic)
          val tp = new TopicPartition(topic, 0)
          (for {
            producer <-
              LowLevelKafkaProducer.resource[IO, String, String](
                TransactionalProducerSettings(
                  transactionID,
                  ProducerSettings[IO, String, String].withProperties(defaultConsumerProperties)
                )
              )
            adminClient <- KafkaAdminClient.resource[IO](adminClientSettings)
          } yield (producer, adminClient))
            .use { case (producer, adminClient) =>
              for {
                _ <- producer
                       .transaction
                       .use { _ =>
                         for {
                           _ <- producer.produce(
                                  Chunk.singleton(
                                    ProducerRecord(
                                      tp.topic(),
                                      "records01-aborted-tx",
                                      "records01-aborted-tx"
                                    ).withPartition(tp.partition())
                                  )
                                )
                           producers <- adminClient.describeProducers(tp.some, none)
                           myProducer = producers.headOption.get._2.activeProducers().asScala.head
                           _         <- adminClient.abortTransaction(
                                  tp,
                                  myProducer.producerId(),
                                  myProducer.producerEpoch().toShort,
                                  myProducer.coordinatorEpoch().orElse(-1)
                                )
                         } yield ()
                       }
                _ <-
                  producer
                    .transaction
                    .use { _ =>
                      producer.produce(
                        Chunk.singleton(
                          ProducerRecord(tp.topic(), "records02-success-tx", "records02-success-tx")
                            .withPartition(
                              tp.partition()
                            )
                        )
                      )
                    }
                _        <- IO.blocking(consumer.assign(List(tp).asJava))
                elements <-
                  Stream
                    .repeatEval(IO.blocking(consumer.poll(java.time.Duration.ofMillis(2000))))
                    .map(_.asScala.toList.map(_.key()).map(new String(_)))
                    .evalTap(IO.println)
                    .takeWhile(x => !x.exists(_ == "records02-success-tx"), true)
                    .flatMap(Stream.emits)
                    .compile
                    .toList
              } yield assert(!elements.exists(_ == "records01-aborted-tx"))
            }
            .unsafeRunSync()
        }
      }
    }

    it("should support electLeaders") {
      withTopic { topic =>
        commonSetup(topic)

        KafkaAdminClient
          .resource[IO](adminClientSettings)
          .use { adminClient =>
            val tp = new TopicPartition(topic, 0)
            for {
              elected <- adminClient.electLeaders(ElectionType.PREFERRED, Set(tp)).attempt
              _       <- IO {
                     elected match {
                       case Left(e: ElectionNotNeededException) =>
                         assert(
                           Option(e.getMessage).exists(msg =>
                             msg.contains("not needed") || msg.contains("election")
                           )
                         )
                       case other => fail(s"expected specific failure but got: $other")
                     }
                   }
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

  it("should support metrics") {
    withTopic { topic =>
      commonSetup(topic)
      KafkaAdminClient
        .resource[IO](adminClientSettings)
        .use { adminClient =>
          for {
            metrics <- adminClient.metrics()
            _       <- IO(assert(metrics.nonEmpty))
          } yield println(metrics)
        }
        .unsafeRunSync()
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
