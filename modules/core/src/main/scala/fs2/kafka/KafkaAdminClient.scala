/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.annotation.nowarn

import cats.{Foldable, Functor}
import cats.effect.*
import cats.syntax.all.*
import fs2.kafka.admin.MkAdminClient
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithAdminClient
import fs2.kafka.KafkaAdminClient.*
import fs2.Stream

import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigResource

/**
  * [[KafkaAdminClient]] represents an admin client for Kafka, which is able to describe queries
  * about topics, consumer groups, offsets, and other entities related to Kafka.<br><br>
  *
  * Use [[KafkaAdminClient.resource]] or [[KafkaAdminClient.stream]] to create an instance.
  */
sealed abstract class KafkaAdminClient[F[_]] {

  /**
    * Updates the configuration for the specified resources.
    */
  def alterConfigs[G[_]](configs: Map[ConfigResource, G[AlterConfigOp]])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Increase the number of partitions for different topics
    */
  def createPartitions(newPartitions: Map[String, NewPartitions]): F[Unit]

  /**
    * Creates the specified topic.
    */
  def createTopic(topic: NewTopic): F[Unit]

  /**
    * Creates the specified topics.
    */
  def createTopics[G[_]](topics: G[NewTopic])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Creates the specified ACLs
    */
  def createAcls[G[_]](acls: G[AclBinding])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Deletes the specified topic.
    */
  def deleteTopic(topic: String): F[Unit]

  /**
    * Deletes the specified topics.
    */
  def deleteTopics[G[_]](topics: G[String])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Deletes ACLs based on specified filters
    */
  def deleteAcls[G[_]](filters: G[AclBindingFilter])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Describes the cluster. Returns nodes using:
    *
    * {{{
    * describeCluster.nodes
    * }}}
    *
    * or the controller node using:
    *
    * {{{
    * describeCluster.controller
    * }}}
    *
    * or the cluster ID using the following.
    *
    * {{{
    * describeCluster.clusterId
    * }}}
    */
  def describeCluster: DescribeCluster[F]

  /**
    * Describes the configurations for the specified resources.
    */
  def describeConfigs[G[_]](resources: G[ConfigResource])(implicit
    G: Foldable[G]
  ): F[Map[ConfigResource, List[ConfigEntry]]]

  /**
    * Describes the consumer groups with the specified group ids, returning a `Map` with group ids
    * as keys, and `ConsumerGroupDescription`s as values.
    */
  def describeConsumerGroups[G[_]](groupIds: G[String])(implicit
    G: Foldable[G]
  ): F[Map[String, ConsumerGroupDescription]]

  /**
    * Describes the topics with the specified topic names, returning a `Map` with topic names as
    * keys, and `TopicDescription`s as values.
    */
  def describeTopics[G[_]](topics: G[String])(implicit
    G: Foldable[G]
  ): F[Map[String, TopicDescription]]

  /**
    * Describes the ACLs based on the specified filters, returning a `List` of `AclBinding` entries
    * matched
    */
  def describeAcls(filter: AclBindingFilter): F[List[AclBinding]]

  /**
    * Lists consumer group offsets. Returns offsets per topic-partition using:
    *
    * {{{
    * listConsumerGroupOffsets(groupId)
    *   .partitionsToOffsetAndMetadata
    * }}}
    *
    * or only offsets for specified topic-partitions using the following.
    *
    * {{{
    * listConsumerGroupOffsets(groupId)
    *   .forPartitions(topicPartitions)
    *   .partitionsToOffsetAndMetadata
    * }}}
    */
  def listConsumerGroupOffsets(groupId: String): ListConsumerGroupOffsets[F]

  /**
    * Lists consumer groups. Returns group ids using:
    *
    * {{{
    * listConsumerGroups.groupIds
    * }}}
    *
    * or `ConsumerGroupListing`s using the following.
    *
    * {{{
    * listConsumerGroups.listings
    * }}}
    */
  def listConsumerGroups: ListConsumerGroups[F]

  /**
    * Delete consumer groups from the cluster.
    */
  def deleteConsumerGroups[G[_]](groupIds: G[String])(implicit
    G: Foldable[G]
  ): F[Unit]

  /**
    * Lists topics. Returns topic names using:
    *
    * {{{
    * listTopics.names
    * }}}
    *
    * or `TopicListing`s using:
    *
    * {{{
    * listTopics.listings
    * }}}
    *
    * or a `Map` of topic names to `TopicListing`s using the following.
    *
    * {{{
    * listTopics.namesToListings
    * }}}
    *
    * If you want to include internal topics, first use `includeInternal`.
    *
    * {{{
    * listTopics.includeInternal.listings
    * }}}
    */
  def listTopics: ListTopics[F]

  /**
    * Alters offsets for the specified group. In order to succeed, the group must be empty.
    */
  def alterConsumerGroupOffsets(
    groupId: String,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit]

  /**
    * Delete committed offsets for a set of partitions in a consumer group. This will succeed at the
    * partition level only if the group is not actively subscribed to the corresponding topic.
    */
  def deleteConsumerGroupOffsets(groupId: String, partitions: Set[TopicPartition]): F[Unit]

}

object KafkaAdminClient {

  private[this] def alterConfigsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    configs: Map[ConfigResource, G[AlterConfigOp]]
  ): F[Unit] =
    withAdminClient(_.incrementalAlterConfigs(configs.asJavaMap).all).void

  private[this] def createPartitionsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    newPartitions: Map[String, NewPartitions]
  ): F[Unit] =
    withAdminClient(_.createPartitions(newPartitions.asJava).all).void

  private[this] def createTopicWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    topic: NewTopic
  ): F[Unit] =
    withAdminClient(_.createTopics(java.util.Collections.singleton(topic)).all).void

  private[this] def createTopicsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    topics: G[NewTopic]
  ): F[Unit] =
    withAdminClient(_.createTopics(topics.asJava).all).void

  private[this] def createAclsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    acls: G[AclBinding]
  ): F[Unit] =
    withAdminClient(_.createAcls(acls.asJava).all).void

  private[this] def deleteTopicWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    topic: String
  ): F[Unit] =
    withAdminClient(_.deleteTopics(java.util.Collections.singleton(topic)).all).void

  private[this] def deleteTopicsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    topics: G[String]
  ): F[Unit] =
    withAdminClient(_.deleteTopics(topics.asJava).all).void

  private[this] def deleteAclsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    filters: G[AclBindingFilter]
  ): F[Unit] =
    withAdminClient(_.deleteAcls(filters.asJava).all).void

  sealed abstract class DescribeCluster[F[_]] {

    /**
      * Lists available nodes in the cluster.
      */
    def nodes: F[Set[Node]]

    /**
      * The node in the cluster acting as the current controller.
      */
    def controller: F[Node]

    /**
      * Current cluster ID.
      */
    def clusterId: F[String]

  }

  private[this] def describeClusterWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): DescribeCluster[F] =
    new DescribeCluster[F] {

      override def nodes: F[Set[Node]] =
        withAdminClient(_.describeCluster.nodes).map(_.toSet)

      override def controller: F[Node] =
        withAdminClient(_.describeCluster.controller)

      override def clusterId: F[String] =
        withAdminClient(_.describeCluster.clusterId)

      override def toString: String =
        "DescribeCluster$" + System.identityHashCode(this)

    }

  private[this] def describeConfigsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    resources: G[ConfigResource]
  ): F[Map[ConfigResource, List[ConfigEntry]]] =
    withAdminClient(
      _.describeConfigs(resources.asJava).all
    ).map(
      _.toMap
        .map { case (k, v) =>
          (k, v.entries().toList)
        }
        .toMap
    )

  private[this] def describeConsumerGroupsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    groupIds: G[String]
  ): F[Map[String, ConsumerGroupDescription]] =
    withAdminClient(_.describeConsumerGroups(groupIds.asJava).all).map(_.toMap)

  private[this] def describeTopicsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    topics: G[String]
  ): F[Map[String, TopicDescription]] =
    withAdminClient(_.describeTopics(topics.asJava).allTopicNames).map(_.toMap)

  private[this] def describeAclsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    filter: AclBindingFilter
  ): F[List[AclBinding]] =
    withAdminClient(_.describeAcls(filter).values()).map(_.toList)

  sealed abstract class ListConsumerGroupOffsetsForPartitions[F[_]] {

    /**
      * Lists consumer group offsets on specified partitions for the consumer group.
      */
    def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]]
  }

  private[this] def listConsumerGroupOffsetsForPartitionsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    groupId: String,
    partitions: G[TopicPartition]
  ): ListConsumerGroupOffsetsForPartitions[F] =
    new ListConsumerGroupOffsetsForPartitions[F] {

      private[this] val groupOffsets = Map(
        groupId -> new ListConsumerGroupOffsetsSpec().topicPartitions(partitions.asJava)
      ).asJava

      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        withAdminClient { adminClient =>
          adminClient.listConsumerGroupOffsets(groupOffsets).partitionsToOffsetAndMetadata
        }.map(_.toMap)

      override def toString: String =
        s"ListConsumerGroupOffsetsForPartitions(groupId = $groupId, partitions = $partitions)"

    }

  sealed abstract class ListConsumerGroupOffsets[F[_]] {

    /**
      * Lists consumer group offsets for the consumer group.
      */
    def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]]

    /**
      * Only includes consumer group offsets for specified topic-partitions.
      */
    def forPartitions[G[_]](
      partitions: G[TopicPartition]
    )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F]

  }

  private[this] def listConsumerGroupOffsetsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    groupId: String
  ): ListConsumerGroupOffsets[F] =
    new ListConsumerGroupOffsets[F] {

      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        withAdminClient { adminClient =>
          adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
        }.map(_.toMap)

      override def forPartitions[G[_]](
        partitions: G[TopicPartition]
      )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F] =
        listConsumerGroupOffsetsForPartitionsWith(withAdminClient, groupId, partitions)

      override def toString: String =
        s"ListConsumerGroupOffsets(groupId = $groupId)"

    }

  sealed abstract class ListConsumerGroups[F[_]] {

    /**
      * Lists the available consumer group ids.
      */
    def groupIds: F[List[String]]

    /**
      * List the available consumer groups as `ConsumerGroupListing`s.
      */
    def listings: F[List[ConsumerGroupListing]]

  }

  private[this] def listConsumerGroupsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): ListConsumerGroups[F] =
    new ListConsumerGroups[F] {

      override def groupIds: F[List[String]] =
        withAdminClient(_.listConsumerGroups.all).map(_.mapToList(_.groupId))

      override def listings: F[List[ConsumerGroupListing]] =
        withAdminClient(_.listConsumerGroups.all).map(_.toList)

      override def toString: String =
        "ListConsumerGroups$" + System.identityHashCode(this)

    }

  sealed abstract class ListTopicsIncludeInternal[F[_]] {

    /**
      * Lists topic names. Includes internal topics.
      */
    def names: F[Set[String]]

    /**
      * Lists topics as `TopicListing`s. Includes internal topics.
      */
    def listings: F[List[TopicListing]]

    /**
      * Lists topics as a `Map` from topic names to `TopicListing`s. Includes internal topics.
      */
    def namesToListings: F[Map[String, TopicListing]]

  }

  private[this] def listTopicsIncludeInternalWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): ListTopicsIncludeInternal[F] =
    new ListTopicsIncludeInternal[F] {

      private[this] def options: ListTopicsOptions =
        new ListTopicsOptions().listInternal(true)

      override def names: F[Set[String]] =
        withAdminClient(_.listTopics(options).names).map(_.toSet)

      override def listings: F[List[TopicListing]] =
        withAdminClient(_.listTopics(options).listings).map(_.toList)

      override def namesToListings: F[Map[String, TopicListing]] =
        withAdminClient(_.listTopics(options).namesToListings).map(_.toMap)

      override def toString: String =
        "ListTopicsIncludeInternal$" + System.identityHashCode(this)

    }

  sealed abstract class ListTopics[F[_]] {

    /**
      * Lists topic names.
      */
    def names: F[Set[String]]

    /**
      * Lists topics as `TopicListing`s.
      */
    def listings: F[List[TopicListing]]

    /**
      * Lists topics as a `Map` from topic names to `TopicListing`s.
      */
    def namesToListings: F[Map[String, TopicListing]]

    /**
      * Include internal topics in the listing.
      */
    def includeInternal: ListTopicsIncludeInternal[F]

  }

  private[this] def listTopicsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): ListTopics[F] =
    new ListTopics[F] {

      override def names: F[Set[String]] =
        withAdminClient(_.listTopics.names).map(_.toSet)

      override def listings: F[List[TopicListing]] =
        withAdminClient(_.listTopics.listings).map(_.toList)

      override def namesToListings: F[Map[String, TopicListing]] =
        withAdminClient(_.listTopics.namesToListings).map(_.toMap)

      override def includeInternal: ListTopicsIncludeInternal[F] =
        listTopicsIncludeInternalWith(withAdminClient)

      override def toString: String =
        "ListTopics$" + System.identityHashCode(this)

    }

  private[this] def alterConsumerGroupOffsetsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    groupId: String,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    withAdminClient(_.alterConsumerGroupOffsets(groupId, offsets.asJava).all()).void

  private[this] def deleteConsumerGroupOffsetsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    groupId: String,
    partitions: Set[TopicPartition]
  ): F[Unit] =
    withAdminClient(_.deleteConsumerGroupOffsets(groupId, partitions.asJava).all()).void

  private[this] def deleteConsumerGroupsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    groupIds: G[String]
  ): F[Unit] =
    withAdminClient(_.deleteConsumerGroups(groupIds.asJava).all()).void

  /**
    * Creates a new [[KafkaAdminClient]] in the `Resource` context, using the specified
    * [[AdminClientSettings]]. If working in a `Stream` context, you might prefer
    * [[KafkaAdminClient.stream]].
    */
  def resource[F[_]](
    settings: AdminClientSettings
  )(implicit
    F: Async[F],
    mk: MkAdminClient[F]
  ): Resource[F, KafkaAdminClient[F]] = resourceIn[F, F](settings)(F, F, mk)

  /**
    * Like [[resource]], but allows the effect type of the created [[KafkaAdminClient]] to be
    * different from the effect type of the [[Resource]] that allocates it.
    */
  def resourceIn[F[_], G[_]](
    settings: AdminClientSettings
  )(implicit
    F: Sync[F],
    G: Async[G],
    mk: MkAdminClient[F]
  ): Resource[F, KafkaAdminClient[G]] =
    WithAdminClient[F, G](mk, settings).map(create[G])

  private def create[F[_]: Functor](client: WithAdminClient[F]) =
    new KafkaAdminClient[F] {

      override def alterConfigs[G[_]](configs: Map[ConfigResource, G[AlterConfigOp]])(implicit
        G: Foldable[G]
      ): F[Unit] =
        alterConfigsWith(client, configs)

      override def createPartitions(newPartitions: Map[String, NewPartitions]): F[Unit] =
        createPartitionsWith(client, newPartitions)

      override def createTopic(topic: NewTopic): F[Unit] =
        createTopicWith(client, topic)

      override def createTopics[G[_]](topics: G[NewTopic])(implicit
        G: Foldable[G]
      ): F[Unit] =
        createTopicsWith(client, topics)

      override def createAcls[G[_]](acls: G[AclBinding])(implicit
        G: Foldable[G]
      ): F[Unit] =
        createAclsWith(client, acls)

      override def deleteTopic(topic: String): F[Unit] =
        deleteTopicWith(client, topic)

      override def deleteTopics[G[_]](topics: G[String])(implicit G: Foldable[G]): F[Unit] =
        deleteTopicsWith(client, topics)

      override def deleteAcls[G[_]](filters: G[AclBindingFilter])(implicit
        G: Foldable[G]
      ): F[Unit] =
        deleteAclsWith(client, filters)

      override def describeCluster: DescribeCluster[F] =
        describeClusterWith(client)

      override def describeConfigs[G[_]](resources: G[ConfigResource])(implicit
        G: Foldable[G]
      ): F[Map[ConfigResource, List[ConfigEntry]]] =
        describeConfigsWith(client, resources)

      override def describeConsumerGroups[G[_]](groupIds: G[String])(implicit
        G: Foldable[G]
      ): F[Map[String, ConsumerGroupDescription]] =
        describeConsumerGroupsWith(client, groupIds)

      override def describeTopics[G[_]](topics: G[String])(implicit
        G: Foldable[G]
      ): F[Map[String, TopicDescription]] =
        describeTopicsWith(client, topics)

      override def listConsumerGroupOffsets(groupId: String): ListConsumerGroupOffsets[F] =
        listConsumerGroupOffsetsWith(client, groupId)

      override def listConsumerGroups: ListConsumerGroups[F] =
        listConsumerGroupsWith(client)

      override def listTopics: ListTopics[F] =
        listTopicsWith(client)

      override def describeAcls(filter: AclBindingFilter): F[List[AclBinding]] =
        describeAclsWith(client, filter)

      override def alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map[TopicPartition, OffsetAndMetadata]
      ): F[Unit] =
        alterConsumerGroupOffsetsWith(client, groupId, offsets)

      override def deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set[TopicPartition]
      ): F[Unit] =
        deleteConsumerGroupOffsetsWith(client, groupId, partitions)

      override def deleteConsumerGroups[G[_]](
        groupIds: G[String]
      )(implicit G: Foldable[G]): F[Unit] =
        deleteConsumerGroupsWith(client, groupIds)

      override def toString: String =
        "KafkaAdminClient$" + System.identityHashCode(this)

    }

  /**
    * Creates a new [[KafkaAdminClient]] in the `Stream` context, using the specified
    * [[AdminClientSettings]]. If you're not working in a `Stream` context, you might instead prefer
    * to use the [[KafkaAdminClient.resource]].
    */
  def stream[F[_]](
    settings: AdminClientSettings
  )(implicit F: Async[F], mk: MkAdminClient[F]): Stream[F, KafkaAdminClient[F]] =
    streamIn[F, F](settings)(F, F, mk)

  /**
    * Like [[stream]], but allows the effect type of the created [[KafkaAdminClient]] to be
    * different from the effect type of the [[Stream]] that allocates it.
    */
  def streamIn[F[_], G[_]](
    settings: AdminClientSettings
  )(implicit F: Sync[F], G: Async[G], mk: MkAdminClient[F]): Stream[F, KafkaAdminClient[G]] =
    Stream.resource(KafkaAdminClient.resourceIn(settings)(F, G, mk))

  /*
   * Prevents the default `MkAdminClient` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("msg=never used")
  implicit private def mkAmbig1[F[_]]: MkAdminClient[F] =
    throw new AssertionError("should not be used")

  @nowarn("msg=never used")
  implicit private def mkAmbig2[F[_]]: MkAdminClient[F] =
    throw new AssertionError("should not be used")

}
