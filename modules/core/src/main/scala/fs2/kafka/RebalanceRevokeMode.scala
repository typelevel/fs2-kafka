package fs2.kafka

/**
  * The available options for [[ConsumerSettings#rebalanceRevokeMode]].<br><br>
  *
  * Available options include:<br>
  *   - [[RebalanceRevokeMode.Eager]] old behaviour, to release assigned partition as soon as
  *     possible,<br>
  *   - [[RebalanceRevokeMode.Graceful]] modified behavior, waiting for configured amount of time:
  *     {{{org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG}}} It is
  *     guaranteed by kafka protocol that after that timeout old consumer will be marked as dead.
  *
  * Default mode is [[RebalanceRevokeMode.Eager]] which is exactly the same as old behavior and can
  * be preferred if rebalance need to happen as quickly as possible and having multiple consumers
  * working on a partition for a moment is not a problem.
  *
  * On the other hand if you want stricter guarantees about processing and attempt to wait for
  * existing streams to finish processing messages before releasing partition choose
  * [[RebalanceRevokeMode.Graceful]]. Because stream is signalled to be shutdown in-flight commits
  * might be lost and some messages might be processed again after new assignment.
  */
sealed abstract class RebalanceRevokeMode

object RebalanceRevokeMode {

  private[kafka] case object EagerMode extends RebalanceRevokeMode

  private[kafka] case object GracefulMode extends RebalanceRevokeMode

  /**
    * Old behavior releasing partition as soon as all streams have messages dispatched and signalled
    * termination
    */
  val Eager: RebalanceRevokeMode = EagerMode

  /**
    * Waiting for configured amount of time:<br>
    * {{{org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG}}} or until all
    * the partition streams finish processing (but not waiting for commits to conclude for that
    * partition)
    */
  val Graceful: RebalanceRevokeMode = GracefulMode

}
