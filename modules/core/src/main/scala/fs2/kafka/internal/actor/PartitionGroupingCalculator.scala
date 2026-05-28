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
final case class PartitionGroupingCalculator(
  groupGoal:   Set[Set[TopicPartition]],
  groupRevoke: Set[Set[TopicPartition]]
)

object PartitionGroupingCalculator {

  /** Computes the partition groups for `targetAssignment`.
    *
    * Returns `groupGoal` (groups that should exist after alignment) and `groupRevoke` (existing groups that must be
    * removed).
    *
    * Grouping policy
    *
    * Partitions are split into as many groups as the specified parallelism allows keeping group sizes evenly
    * distributed.
    *
    * Stable groups
    *
    * An existing group is left out of `groupRevoke` when all of its partitions are still in `targetAssignment` and its
    * size matches the target default or oversized size.
    *
    * Revocation
    *
    * `groupRevoke` contains existing groups where:
    *   - at least one partition is no longer assigned, or
    *   - the group's size no longer matches the target layout
    */
  private[actor] def align(
    targetAssignment:   Set[TopicPartition],
    existingAssignment: Set[Set[TopicPartition]],
    maxParallelism:     Int
  ): PartitionGroupingCalculator =
    if (targetAssignment == existingAssignment.flatten) {
      PartitionGroupingCalculator(
        existingAssignment,
        Set.empty
      )
    } else if (targetAssignment.isEmpty) {
      PartitionGroupingCalculator(
        Set.empty,
        existingAssignment
      )
    } else {
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
        .toSet

      val defaultSizeGroups   = (leftUnassigned -- oversizedGroups.flatten)
        .grouped(defaultGroupSize)
        .toSet
      PartitionGroupingCalculator(
        groupGoal   = groupsToKeepWSpillover ++ groupsToKeepRegularSize ++  defaultSizeGroups ++ (oversizedGroups),
        groupRevoke = unassignDueToPartitionRevoked ++ toRevokeDueToSize
      )
    }

}
