---
id: admin
title: Admin
---

There is partial support for the Kafka admin API through [KafkaAdminClient][kafkaadminclient]. Internally, this relies on the Java Kafka
[AdminClient][admin-client] and supports the same settings.

The following imports are assumed throughout this page.

```scala mdoc:silent
import cats.effect._
import cats.syntax.all._
import fs2.kafka._
import fs2.Stream
```

## Settings

[`AdminClientSettings`][adminclientsettings] is provided to avoid having to deal with `String` key-value settings.

### Default Settings

There are several settings specific to the library.

- `withCloseTimeout` controls the timeout when waiting for admin client shutdown. Default is 20 seconds.

## Client Creation

Once settings are defined, we can use create an admin client in a `Stream`.

```scala mdoc:silent
def kafkaAdminClientStream[F[_]: Async](
  bootstrapServers: String
): Stream[F, KafkaAdminClient[F]] =
  KafkaAdminClient.stream[F](AdminClientSettings(bootstrapServers))
```

Alternatively, we can create an admin client in a `Resource` context.

```scala mdoc:silent
def kafkaAdminClientResource[F[_]: Async](
  bootstrapServers: String
): Resource[F, KafkaAdminClient[F]] =
  KafkaAdminClient.resource[F](AdminClientSettings(bootstrapServers))
```

## Topics

There are functions available for describing, creating, and deleting topics.

```scala mdoc:silent
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}

def topicOperations[F[_]: Async]: F[Unit] =
  kafkaAdminClientResource[F]("localhost:9092").use { client =>
    for {
      topicNames <- client.listTopics.names
      _          <- client.describeTopics(topicNames.toList)
      _          <- client.createTopic(new NewTopic("new-topic", 1, 1.toShort))
      _          <- client.createTopics(new NewTopic("newer-topic", 1, 1.toShort) :: Nil)
      _          <- client.createPartitions(Map("new-topic" -> NewPartitions.increaseTo(4)))
      _          <- client.deleteTopic("new-topic")
      _          <- client.deleteTopics("newer-topic" :: Nil)
    } yield ()
  }
```

## Configurations

We can edit the configuration of different resources, like topics and nodes.

```scala mdoc:silent
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource

def configOperations[F[_]: Async]: F[Unit] =
  kafkaAdminClientResource[F]("localhost:9092").use { client =>
    val topic = new ConfigResource(ConfigResource.Type.TOPIC, "topic")

    for {
      _ <- client.describeConfigs(topic :: Nil)
      _ <- client.alterConfigs {
             Map(
               topic -> List(
                 new AlterConfigOp(
                   new ConfigEntry("cleanup.policy", "delete"),
                   AlterConfigOp.OpType.SET
                 )
               )
             )
           }
    } yield ()
  }
```

## Cluster Metadata

It's possible to retrieve metadata about the cluster nodes.

```scala mdoc:silent
import org.apache.kafka.common.Node

def clusterNodes[F[_]: Async]: F[Set[Node]] =
  kafkaAdminClientResource[F]("localhost:9092").use(_.describeCluster.nodes)
```

## Consumer Groups

There are functions available for working with consumer groups.

```scala mdoc:silent
def consumerGroupOperations[F[_]: Async: cats.Parallel]: F[Unit] =
  kafkaAdminClientResource[F]("localhost:9092").use { client =>
    for {
      consumerGroupIds <- client.listConsumerGroups.groupIds
      _                <- client.describeConsumerGroups(consumerGroupIds)
      _                <- consumerGroupIds.parTraverse { groupId =>
             client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
           }
    } yield ()
  }
```

## ACLs

There are ACL management functions to describe, create and delete ACL entries.

```scala mdoc:silent
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}

def aclOperations[F[_]: Async]: F[Unit] =
  kafkaAdminClientResource[F]("localhost:9092").use { client =>
    for {
      describedAcls <- client.describeAcls(AclBindingFilter.ANY)

      aclEntry = new AccessControlEntry(
                   "User:ANONYMOUS",
                   "*",
                   AclOperation.DESCRIBE,
                   AclPermissionType.ALLOW
                 )
      pattern = new ResourcePattern(ResourceType.TOPIC, "topic1", PatternType.LITERAL)
      acl     = new AclBinding(pattern, aclEntry)
      _      <- client.createAcls(List(acl))

      _ <- client.deleteAcls(List(AclBindingFilter.ANY))
    } yield ()
  }
```

[kafkaadminclient]: @API_BASE_URL@/KafkaAdminClient.html
[adminclientsettings]: @API_BASE_URL@/AdminClientSettings.html
[admin-client]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/admin/AdminClient.html
