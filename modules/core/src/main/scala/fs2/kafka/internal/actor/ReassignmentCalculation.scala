package fs2.kafka.internal.actor

import org.apache.kafka.common.TopicPartition

/** When a rebalance happens, Fs2-Kafka will calculate the
  *
  * Note: goal and revoke might have overlapping partitions.
  *
  * @param add
  * @param revokeFull
  * @param revokePartially
  */
final case class ReassignmentCalculation(
  groupGoal:        Set[Set[TopicPartition]],
  groupRevoke:      Set[Set[TopicPartition]],
  targetAssignment: Set[TopicPartition]
)

object ReassignmentCalculation {

  /** On rebalance, we want to guarantee that we correctly revoke unassigned partitions and correctly stream newly
    * allocated ones.
    *
    * To achieve max parallelization within the specified bounds, we want to make sure we group partitions in as many
    * groups as possible while being bellow the maxParallelism
    *
    * When the number of partitions is not divisible by maxParallelism, we want to distribute the remaining partitions
    * evenly through the groups.
    *
    * This means we want to guarantee all groups have either the same size or differ by at most one partition (the
    * result of distributing the division remainder).
    *
    * Rather than creating a new grouping on every call, the method includes an optimization that will maintain
    * groups composed of assigned partitions that already have the correct size.
    *
    * Groups are classified as revoked if:
    *   - one of their partitions was unassigned
    *   - the count is bellow/above the optimal group sizing
    *
    * Groups are kept if:
    *   - they are part of the assignment AND
    *   - they are part of the group with an appropriate number of partitions
    */
  def align(
    targetAssignment:   Set[TopicPartition],
    existingAssignment: Set[Set[TopicPartition]],
    maxParallelism:     Int
  ): ReassignmentCalculation = {
    val totalGroupCount     = Math.min(targetAssignment.size, maxParallelism)
    val spilloverGroupCount = targetAssignment.size % totalGroupCount

    val defaultGroupSize   = targetAssignment.size / totalGroupCount
    val oversizedGroupSize = defaultGroupSize + 1

    val (stillAssigned, unassignDueToPartitionRevoked) =
      existingAssignment.partition(_.forall(targetAssignment.contains))

    val groupsToKeepWSpillover = stillAssigned
      .filter(_.size == oversizedGroupSize)
      .take(spilloverGroupCount)

    val (groupsToKeepRegularSize, toRevokeDueToSize) = stillAssigned
      .diff(groupsToKeepWSpillover)
      .partition(_.size == defaultGroupSize)

    val leftUnassigned =
      targetAssignment -- (groupsToKeepWSpillover.flatten ++ groupsToKeepRegularSize.flatten)

    val oversizedGroups = leftUnassigned
      .grouped(oversizedGroupSize)
      .take(spilloverGroupCount - groupsToKeepWSpillover.size)
      .toList

    val defaultSizeGroups = (leftUnassigned -- oversizedGroups.flatten)
      .grouped(defaultGroupSize)
      .toSet

    ReassignmentCalculation(
      groupGoal        = defaultSizeGroups ++ oversizedGroups,
      groupRevoke      = unassignDueToPartitionRevoked ++ toRevokeDueToSize,
      targetAssignment = targetAssignment
    )
  }
}
