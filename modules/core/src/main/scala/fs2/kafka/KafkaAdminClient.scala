/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.time.Instant

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration

import cats.effect.*
import cats.syntax.all.*
import cats.Foldable
import cats.Functor
import fs2.kafka.admin.MkAdminClient
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithAdminClient
import fs2.kafka.KafkaAdminClient.*
import fs2.Chunk
import fs2.Stream

import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.Uuid

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

  /**
    * Incrementally update the configuration for the specified resources
    */
  def alterConfigs[G[_]: Foldable](
    configs: Map[ConfigResource, G[AlterConfigOp]],
    validateOnly: Boolean
  ): F[Unit]

  /**
    * Change the log directory for the specified replicas.
    */
  def alterReplicaLogDirs(replicaAssignment: Map[TopicPartitionReplica, String]): F[Unit]

  /**
    * Query the information of all log directories on the given set of brokers.
    */
  def describeLogDirs[G[_]: Foldable: Functor](
    brokers: G[Int]
  ): F[Map[Int, Map[String, LogDirDescription]]]

  /**
    * Query the replica log directory information for the specified replicas.
    */
  def describeReplicaLogDirs[G[_]: Foldable](
    replicas: G[TopicPartitionReplica]
  ): F[Map[TopicPartitionReplica, ReplicaLogDirInfo]]

  /**
    * Delete records whose offset is smaller than the given offset of the corresponding partition.
    */
  def deleteRecords(recordsToDelete: Map[TopicPartition, RecordsToDelete]): F[Unit]

  /**
    * Creates a delegation token
    */
  def createDelegationToken(
    renewers: List[KafkaPrincipal],
    owner: Option[KafkaPrincipal],
    maxLifeTime: Option[FiniteDuration]
  ): F[DelegationToken]

  /**
    * Expire a delegation token, returning the expiry timestamp.
    */
  def expireDelegationToken(hmac: Chunk[Byte], expiryTime: Option[FiniteDuration]): F[Instant]

  /**
    * Renew a delegation token and return the expiry timestamp.
    */
  def renewDelegationToken(hmac: Chunk[Byte], renewTime: Option[FiniteDuration]): F[Instant]

  /**
    * Describe the delegation tokens.
    */
  def describeDelegationToken[G[_]: Foldable](
    owners: Option[G[KafkaPrincipal]]
  ): F[List[DelegationToken]]

  /**
    * Elect a replica as leader for topic partitions.
    */
  def electLeaders(electionType: ElectionType, partitions: Set[TopicPartition]): F[Unit]

  /**
    * Change the reassignments for one or more partitions.
    */
  def alterPartitionReassignments(
    reassignments: Map[TopicPartition, Option[NewPartitionReassignment]]
  ): F[Unit]

  /**
    * List the current reassignments for the given partitions.
    */
  def listPartitionReassignments(
    partitionsFilter: Option[Set[TopicPartition]]
  ): F[Map[TopicPartition, PartitionReassignment]]

  /**
    * Remove members from the consumer group by given member identities.
    */
  def removeMembersFromConsumerGroup[G[_]: Foldable](
    groupId: String,
    members: G[MemberToRemove],
    reason: Option[String]
  ): F[Unit]

  /**
    * List offset for the specified partitions and isolation level.
    */
  def listOffsets(
    topicPartitionOffsets: Map[TopicPartition, OffsetSpec],
    isolationLevel: org.apache.kafka.common.IsolationLevel
  ): F[Map[TopicPartition, ListOffsetsResultInfo]]

  /**
    * Describes all entities matching the provided filter that have at least one client quota
    * configuration value defined.
    */
  def describeClientQuotas(
    filter: ClientQuotaFilter
  ): F[Map[ClientQuotaEntity, Map[String, Double]]]

  /**
    * Alters client quota configurations with the specified alterations.
    */
  def alterClientQuotas[G[_]: Foldable](entries: G[ClientQuotaAlteration]): F[Unit]

  /**
    * Describe all SASL/SCRAM credentials.
    */
  def describeUserScramCredentials(
    users: Option[List[String]] = None
  ): F[Map[String, UserScramCredentialsDescription]]

  /**
    * Describes finalized as well as supported features.
    */
  def describeFeatures(): F[FeatureMetadata]

  /**
    * Applies specified updates to finalized features.
    */
  def updateFeatures(features: Map[String, FeatureUpdate], validateOnly: Boolean): F[Unit]

  /**
    * Describes the state of the metadata quorum.
    */
  def describeMetadataQuorum(): F[QuorumInfo]

  /**
    * Describe producer state on a set of topic partitions.
    */
  def describeProducers[G[_]: Foldable](
    partitions: G[TopicPartition],
    brokerId: Option[Int]
  ): F[Map[TopicPartition, PartitionProducerState]]

  /**
    * Describe the state of a set of transactional IDs from the respective transaction coordinators,
    * which are dynamically discovered.
    */
  def describeTransactions[G[_]: Foldable](
    transactionalIds: G[String]
  ): F[Map[String, TransactionDescription]]

  /**
    * Forcefully abort a transaction which is open on a topic partition.
    */
  def abortTransaction(
    topicPartition: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    coordinationEpoch: Int
  ): F[Unit]

  /**
    * List active transactions in the cluster.
    */
  def listTransactions(
    states: Option[Set[TransactionState]],
    producerIds: Option[Set[Long]],
    duration: Option[Long]
  ): F[List[TransactionListing]]

  /**
    * Fence out all active producers that use any of the provided transactional IDs.
    */
  def fenceProducers[G[_]: Foldable](transactionalIds: G[String]): F[Unit]

  /**
    * @return
    */
  def listConfigResources(): F[List[ConfigResource]]

  /**
    * Add a new voter node to the KRaft metadata quorum.
    */
  def addRaftVoter(
    voterId: Int,
    voterDirectoryId: org.apache.kafka.common.Uuid,
    endpoints: Set[RaftVoterEndpoint],
    clusterId: Option[String]
  ): F[Unit]

  /**
    * Remove a voter node from the KRaft metadata quorum.
    */
  def removeRaftVoter(
    voterId: Int,
    voterDirectoryId: org.apache.kafka.common.Uuid,
    clusterId: Option[String]
  ): F[Unit]

  /**
    * Get the metrics kept by the adminClient
    */
  def metrics(): F[Map[MetricName, Metric]]

}

object KafkaAdminClient {

  private[this] def alterConfigsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    configs: Map[ConfigResource, G[AlterConfigOp]],
    validateOnly: Boolean
  ): F[Unit] =
    withAdminClient { client =>
      val options: AlterConfigsOptions = new AlterConfigsOptions().validateOnly(validateOnly)
      client.incrementalAlterConfigs(configs.asJavaMap, options).all
    }.void

  private[this] def alterReplicaLogDirsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    replicaAssignment: Map[TopicPartitionReplica, String]
  ): F[Unit] =
    withAdminClient(_.alterReplicaLogDirs(replicaAssignment.asJava).all).void

  private[this] def describeLogDirsWith[F[_]: Functor, G[_]: Foldable: Functor](
    withAdminClient: WithAdminClient[F],
    brokers: G[Int]
  ): F[Map[Int, Map[String, LogDirDescription]]] =
    withAdminClient(_.describeLogDirs(brokers.map(Integer.valueOf).asJava).allDescriptions()).map(
      _.asScala.view.map { case (k, v) => k.toInt -> v.asScala.toMap }.toMap
    )

  private[this] def describeReplicaLogDirsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    replicas: G[TopicPartitionReplica]
  ): F[Map[TopicPartitionReplica, ReplicaLogDirInfo]] =
    withAdminClient(_.describeReplicaLogDirs(replicas.asJava).all).map(_.asScala.toMap)

  private[this] def deleteRecordsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    recordsToDelete: Map[TopicPartition, RecordsToDelete]
  ): F[Unit] =
    withAdminClient(_.deleteRecords(recordsToDelete.asJava).all).void

  private[this] def createDelegationTokenWith[F[_]](
    withAdminClient: WithAdminClient[F],
    renewers: List[KafkaPrincipal],
    owner: Option[KafkaPrincipal],
    maxLifeTime: Option[FiniteDuration]
  ): F[DelegationToken] = {
    val options = new CreateDelegationTokenOptions()
    options.renewers(renewers.asJava)
    maxLifeTime.map(_.toMillis).foreach(options.maxLifetimeMs)
    owner.foreach(options.owner)
    withAdminClient(_.createDelegationToken(options).delegationToken())
  }

  private[this] def expireDelegationTokenWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    hmac: Chunk[Byte],
    expiryTime: Option[FiniteDuration]
  ): F[Instant] = {
    val options = new ExpireDelegationTokenOptions()
    expiryTime.foreach(expiryTime => options.expiryTimePeriodMs(expiryTime.toMillis))
    withAdminClient(_.expireDelegationToken(hmac.toArray, options).expiryTimestamp()).map(
      expiryTimestamp => Instant.ofEpochMilli(expiryTimestamp.toLong)
    )
  }

  private[this] def renewDelegationTokenWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    hmac: Chunk[Byte],
    renewTime: Option[FiniteDuration]
  ): F[Instant] = {
    val options = new RenewDelegationTokenOptions()
    renewTime.foreach(renewTime => options.renewTimePeriodMs(renewTime.toMillis))
    withAdminClient(_.renewDelegationToken(hmac.toArray, options).expiryTimestamp()).map(
      expiryTimestamp => Instant.ofEpochMilli(expiryTimestamp.toLong)
    )
  }

  private[this] def describeDelegationTokenWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    owners: Option[G[KafkaPrincipal]]
  ): F[List[DelegationToken]] = {
    val options = new DescribeDelegationTokenOptions()
    owners.map(_.toList.asJava).foreach(options.owners)
    withAdminClient(_.describeDelegationToken(options).delegationTokens()).map(_.asScala.toList)
  }

  private[this] def electLeadersWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    electionType: ElectionType,
    partitions: Set[TopicPartition]
  ): F[Unit] =
    withAdminClient(_.electLeaders(electionType, partitions.asJava).all).void

  private[this] def alterPartitionReassignmentsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    reassignments: Map[TopicPartition, Option[NewPartitionReassignment]]
  ): F[Unit] = {
    val javaOpts = reassignments.view.map { case (k, v) => k -> v.toJava }.toMap.asJava
    withAdminClient(_.alterPartitionReassignments(javaOpts).all).void
  }

  private[this] def listPartitionReassignmentsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    partitions: Option[Set[TopicPartition]]
  ): F[Map[TopicPartition, PartitionReassignment]] =
    withAdminClient { client =>
      partitions
        .fold(client.listPartitionReassignments())(partitions =>
          client.listPartitionReassignments(partitions.asJava)
        )
        .reassignments()
    }.map(_.asScala.toMap)

  private[this] def removeMembersFromConsumerGroupWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    groupId: String,
    members: G[MemberToRemove],
    reason: Option[String]
  ): F[Unit] = {
    val options = new RemoveMembersFromConsumerGroupOptions(members.toList.asJava)
    reason.foreach(options.reason)
    withAdminClient(_.removeMembersFromConsumerGroup(groupId, options).all).void
  }

  private[this] def listOffsetsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    topicPartitionOffsets: Map[TopicPartition, OffsetSpec],
    isolationLevel: org.apache.kafka.common.IsolationLevel
  ): F[Map[TopicPartition, ListOffsetsResultInfo]] = {
    val opts = new ListOffsetsOptions(isolationLevel)
    withAdminClient(_.listOffsets(topicPartitionOffsets.asJava, opts).all).map(_.asScala.toMap)
  }

  private[this] def describeClientQuotasWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    filter: ClientQuotaFilter
  ): F[Map[ClientQuotaEntity, Map[String, Double]]] =
    withAdminClient(_.describeClientQuotas(filter).entities()).map { entities =>
      entities
        .asScala
        .view
        .map { case (k, v) =>
          k -> v.asScala.view.map { case (qk, qv) => qk -> qv.doubleValue }.toMap
        }
        .toMap
    }

  private[this] def alterClientQuotasWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    entries: G[ClientQuotaAlteration]
  ): F[Unit] =
    withAdminClient(_.alterClientQuotas(entries.asJava).all).void

  private[this] def describeUserScramCredentialsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    users: Option[List[String]]
  ): F[Map[String, UserScramCredentialsDescription]] =
    // underlying api uses null to distinguish between no user filter and an empty user filter
    withAdminClient(_.describeUserScramCredentials(users.map(_.asJava).orNull).all).map(
      _.asScala.toMap
    )

  private[this] def describeFeaturesWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): F[FeatureMetadata] =
    withAdminClient(_.describeFeatures().featureMetadata())

  private[this] def updateFeaturesWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    features: Map[String, FeatureUpdate],
    validateOnly: Boolean
  ): F[Unit] = {
    val opts = new UpdateFeaturesOptions().validateOnly(validateOnly)
    withAdminClient(_.updateFeatures(features.asJava, opts).all).void
  }

  private[this] def describeMetadataQuorumWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): F[QuorumInfo] =
    withAdminClient(_.describeMetadataQuorum().quorumInfo())

  private[this] def describeProducersWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    partitions: G[TopicPartition],
    brokerId: Option[Int]
  ): F[Map[TopicPartition, PartitionProducerState]] = {
    val opts = brokerId.foldLeft(new DescribeProducersOptions())((opt, id) => opt.brokerId(id))
    withAdminClient(_.describeProducers(partitions.asJava, opts).all).map(_.asScala.toMap)
  }

  private[this] def describeTransactionsWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    transactionalIds: G[String]
  ): F[Map[String, TransactionDescription]] =
    withAdminClient(_.describeTransactions(transactionalIds.asJava).all).map(_.asScala.toMap)

  private[this] def abortTransactionWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    topicPartition: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    coordinationEpoch: Int
  ): F[Unit] = {
    val spec =
      new AbortTransactionSpec(topicPartition, producerId, producerEpoch, coordinationEpoch)
    withAdminClient(_.abortTransaction(spec).all).void
  }

  private[this] def listTransactionsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    states: Option[Set[TransactionState]],
    producerIds: Option[Set[Long]],
    duration: Option[Long]
  ): F[List[TransactionListing]] = {
    val options = new ListTransactionsOptions()
    val wState  = states.map(_.asJava).fold(options)(options.filterStates)
    val wProd   = producerIds
      .map(_.map(java.lang.Long.valueOf).asJava)
      .fold(wState)(wState.filterProducerIds)
    val wDuration = duration.fold(wProd)(wProd.filterOnDuration)
    withAdminClient(_.listTransactions(wDuration).all).map(_.asScala.toList)
  }

  private[this] def fenceProducersWith[F[_]: Functor, G[_]: Foldable](
    withAdminClient: WithAdminClient[F],
    transactionalIds: G[String]
  ): F[Unit] =
    withAdminClient(_.fenceProducers(transactionalIds.asJava).all).map(_ => ())

  private[this] def listConfigResourcesWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): F[List[ConfigResource]] = withAdminClient(_.listConfigResources().all()).map(_.asScala.toList)

  private[this] def addRaftVoterWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    voterId: Int,
    voterDirectoryId: org.apache.kafka.common.Uuid,
    endpoints: Set[RaftVoterEndpoint],
    clusterId: Option[String]
  ): F[Unit] = {

    withAdminClient(
      _.addRaftVoter(
          voterId,
          voterDirectoryId,
          endpoints.asJava,
          new AddRaftVoterOptions().setClusterId(clusterId.toJava)
        )
        .all
    ).void
  }

  private[this] def removeRaftVoterWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F],
    voterId: Int,
    voterDirectoryId: org.apache.kafka.common.Uuid,
    clusterId: Option[String]
  ): F[Unit] =
    withAdminClient(
      _.removeRaftVoter(
          voterId,
          voterDirectoryId,
          new RemoveRaftVoterOptions().setClusterId(clusterId.toJava)
        )
        .all
    ).map(_ => ())

  private[this] def metricsWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): F[Map[MetricName, Metric]] =
    withAdminClient { client =>
      KafkaFuture.completedFuture[Map[MetricName, Metric]](client.metrics().asScala.toMap)
    }

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
      * List the available consumer groups as `GroupListing`s.
      */
    def listings: F[List[GroupListing]]

  }

  private[this] def listConsumerGroupsWith[F[_]: Functor](
    withAdminClient: WithAdminClient[F]
  ): ListConsumerGroups[F] =
    new ListConsumerGroups[F] {

      override def groupIds: F[List[String]] =
        withAdminClient(_.listGroups().all).map(_.mapToList(_.groupId))

      override def listings: F[List[GroupListing]] =
        withAdminClient(_.listGroups().all).map(_.toList)

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
        alterConfigsWith(client, configs, validateOnly = false)

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

      override def alterConfigs[G[_]: Foldable](
        configs: Map[ConfigResource, G[AlterConfigOp]],
        validateOnly: Boolean
      ): F[Unit] =
        alterConfigsWith[F, G](client, configs, validateOnly)

      override def alterReplicaLogDirs(
        replicaAssignment: Map[TopicPartitionReplica, String]
      ): F[Unit] =
        alterReplicaLogDirsWith[F](client, replicaAssignment)

      override def describeLogDirs[G[_]: Foldable: Functor](
        brokers: G[Int]
      ): F[Map[Int, Map[String, LogDirDescription]]] =
        describeLogDirsWith[F, G](client, brokers)

      override def describeReplicaLogDirs[G[_]: Foldable](
        replicas: G[TopicPartitionReplica]
      ): F[Map[TopicPartitionReplica, ReplicaLogDirInfo]] =
        describeReplicaLogDirsWith[F, G](client, replicas)

      override def deleteRecords(recordsToDelete: Map[TopicPartition, RecordsToDelete]): F[Unit] =
        deleteRecordsWith[F](client, recordsToDelete)

      override def createDelegationToken(
        renewers: List[KafkaPrincipal],
        owner: Option[KafkaPrincipal],
        maxLifeTime: Option[FiniteDuration]
      ): F[DelegationToken] =
        createDelegationTokenWith[F](client, renewers, owner, maxLifeTime)

      override def expireDelegationToken(
        hmac: Chunk[Byte],
        expiryTime: Option[FiniteDuration]
      ): F[Instant] =
        expireDelegationTokenWith(client, hmac, expiryTime)

      override def renewDelegationToken(
        hmac: Chunk[Byte],
        renewTime: Option[FiniteDuration]
      ): F[Instant] =
        renewDelegationTokenWith(client, hmac, renewTime)

      override def describeDelegationToken[G[_]: Foldable](
        owners: Option[G[KafkaPrincipal]]
      ): F[List[DelegationToken]] =
        describeDelegationTokenWith[F, G](client, owners)

      override def electLeaders(
        electionType: ElectionType,
        partitions: Set[TopicPartition]
      ): F[Unit] =
        electLeadersWith[F](client, electionType, partitions)

      override def alterPartitionReassignments(
        reassignments: Map[TopicPartition, Option[NewPartitionReassignment]]
      ): F[Unit] =
        alterPartitionReassignmentsWith[F](client, reassignments)

      override def listPartitionReassignments(
        partitionsFilter: Option[Set[TopicPartition]]
      ): F[Map[TopicPartition, PartitionReassignment]] =
        listPartitionReassignmentsWith(client, partitionsFilter)

      override def removeMembersFromConsumerGroup[G[_]: Foldable](
        groupId: String,
        members: G[MemberToRemove],
        reason: Option[String]
      ): F[Unit] =
        removeMembersFromConsumerGroupWith(client, groupId, members, reason)

      override def listOffsets(
        topicPartitionOffsets: Map[TopicPartition, OffsetSpec],
        isolationLevel: common.IsolationLevel
      ): F[Map[TopicPartition, ListOffsetsResultInfo]] =
        listOffsetsWith(client, topicPartitionOffsets, isolationLevel)

      override def describeClientQuotas(
        filter: ClientQuotaFilter
      ): F[Map[ClientQuotaEntity, Map[String, Double]]] =
        describeClientQuotasWith(client, filter)

      override def alterClientQuotas[G[_]: Foldable](entries: G[ClientQuotaAlteration]): F[Unit] =
        alterClientQuotasWith[F, G](client, entries)

      override def describeUserScramCredentials(
        users: Option[List[String]]
      ): F[Map[String, UserScramCredentialsDescription]] =
        describeUserScramCredentialsWith(client, users)

      override def describeFeatures(): F[FeatureMetadata] =
        describeFeaturesWith(client)

      override def updateFeatures(
        features: Map[String, FeatureUpdate],
        validateOnly: Boolean
      ): F[Unit] = updateFeaturesWith(client, features, validateOnly)

      override def describeMetadataQuorum(): F[QuorumInfo] =
        describeMetadataQuorumWith(client)

      override def describeProducers[G[_]: Foldable](
        partitions: G[TopicPartition],
        brokerId: Option[Int]
      ): F[Map[TopicPartition, PartitionProducerState]] =
        describeProducersWith(client, partitions, brokerId)

      override def describeTransactions[G[_]: Foldable](
        transactionalIds: G[String]
      ): F[Map[String, TransactionDescription]] =
        describeTransactionsWith(client, transactionalIds)

      override def abortTransaction(
        topicPartition: TopicPartition,
        producerId: Long,
        producerEpoch: Short,
        coordinationEpoch: Int
      ): F[Unit] =
        abortTransactionWith(client, topicPartition, producerId, producerEpoch, coordinationEpoch)

      override def listTransactions(
        states: Option[Set[TransactionState]],
        producerIds: Option[Set[Long]],
        duration: Option[Long]
      ): F[List[TransactionListing]] =
        listTransactionsWith(client, states, producerIds, duration)

      override def fenceProducers[G[_]: Foldable](transactionalIds: G[String]): F[Unit] =
        fenceProducersWith(client, transactionalIds)

      override def listConfigResources(): F[List[ConfigResource]] =
        listConfigResourcesWith[F](client)

      override def addRaftVoter(
        voterId: Int,
        voterDirectoryId: Uuid,
        endpoints: Set[RaftVoterEndpoint],
        clusterId: Option[String]
      ): F[Unit] =
        addRaftVoterWith(client, voterId, voterDirectoryId, endpoints, clusterId)

      override def removeRaftVoter(
        voterId: Int,
        voterDirectoryId: Uuid,
        clusterId: Option[String]
      ): F[Unit] =
        removeRaftVoterWith(client, voterId, voterDirectoryId, clusterId)

      override def metrics: F[Map[MetricName, Metric]] =
        metricsWith(client)

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
