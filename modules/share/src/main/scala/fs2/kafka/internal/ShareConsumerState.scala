/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.Chunk
import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.ShareConsumerState.*
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering

/**
  * A delivery of a record Java holds for the consumer.
  *
  * @param record
  *   the Java record to acknowledge it by, from the poll which last returned it
  * @param heldSince
  *   when the consumer took the poll, or the user's latest renewal, from which the max processing
  *   time counts
  */
final private[kafka] case class Delivery(
  record: KafkaByteConsumerRecord,
  deliveryCount: Short,
  heldSince: FiniteDuration
)

/**
  * What the scheduler reads of the held records to compute its deadlines.
  */
sealed private[kafka] trait HeldRecords {

  def lock: Option[FiniteDuration]

  def nextOverdue(max: FiniteDuration): Option[FiniteDuration]

  def nextRenewal(interval: Option[FiniteDuration]): Option[FiniteDuration]

  def nextSend(delay: Option[FiniteDuration], paused: Boolean): Option[FiniteDuration]

}

/**
  * The records Java holds for the consumer, each with its status. Every change to an entry goes
  * through `updated`, which keeps the counts and the index in step, so that no deadline needs a
  * scan.
  *
  * @param open
  *   whether `process` still has the poll, so that its records can be acknowledged
  * @param unacked
  *   how many records have no acknowledgement, user renewals included
  * @param unsent
  *   the records with an acknowledgement to send
  * @param renewals
  *   how many records the user renewed, which the next renewal takes
  * @param unsentSince
  *   when the oldest acknowledgement to send was recorded
  * @param timed
  *   the records without any acknowledgement, user renewals excluded, by `heldSince`
  * @param lockedSince
  *   the estimated start of the lock: the poll's start or the latest renewal
  * @param lock
  *   the acquisition lock last reported by the broker
  */
final private[kafka] case class ShareConsumerState(
  open: Boolean,
  records: Map[TopicPartitionOffset, Entry],
  unacked: Int,
  unsent: Set[TopicPartitionOffset],
  renewals: Int,
  unsentSince: Option[FiniteDuration],
  timed: SortedSet[(FiniteDuration, TopicPartitionOffset)],
  lockedSince: FiniteDuration,
  lock: Option[FiniteDuration]
) extends HeldRecords {

  /**
    * A new poll, with the lock the broker reported, or the last one known.
    */
  def hold(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    start: FiniteDuration,
    reportedLock: Option[FiniteDuration]
  ): ShareConsumerState =
    ShareConsumerState.empty
      .copy(open = true, lockedSince = start, lock = reportedLock.orElse(lock))
      .withStatuses(deliveries.map { case (p, d) => p -> Some(Entry(d, Unacked)) }, start)

  def renewing: Boolean = records.size > unacked + unsent.size

  def holds(offset: TopicPartitionOffset, deliveryCount: Short): Boolean =
    open && records.get(offset).exists(_.delivery.deliveryCount == deliveryCount)

  /**
    * A renewal the user asked for is due at once.
    */
  def nextRenewal(interval: Option[FiniteDuration]): Option[FiniteDuration] =
    if (renewals > 0) Some(Duration.Zero)
    else interval.filter(_ => unacked > 0).map(lockedSince + _)

  /**
    * Due at once while paused, as no poll follows. Once the poll is closed, the next poll, the
    * final commit or the end of `consume` sends it.
    */
  def nextSend(delay: Option[FiniteDuration], paused: Boolean): Option[FiniteDuration] =
    unsentSince.flatMap(since => if (paused) Some(since) else if (open) delay.map(since + _) else None)

  def nextOverdue(max: FiniteDuration): Option[FiniteDuration] =
    timed.headOption.map { case (since, _) => since + max }

  /**
    * The only change of an entry, keeping the counts and the index in step.
    */
  private def updated(
    offset: TopicPartitionOffset,
    next: Option[Entry],
    at: FiniteDuration
  ): ShareConsumerState = {
    val previous = records.get(offset)
    val unsentNow = if (next.exists(_.unsent > 0)) unsent + offset else unsent - offset
    copy(
      records = next.fold(records - offset)(records.updated(offset, _)),
      unacked = unacked + previous.fold(0)(-_.unacked) + next.fold(0)(_.unacked),
      unsent = unsentNow,
      renewals = renewals + previous.fold(0)(-_.renewal) + next.fold(0)(_.renewal),
      unsentSince =
        if (unsentNow.isEmpty) None
        else if (unsentNow.size > unsent.size) unsentSince.orElse(Some(at))
        else unsentSince,
      timed = timed -- previous.flatMap(_.timed(offset)) ++ next.flatMap(_.timed(offset))
    )
  }

  private def withStatuses(
    changes: Iterable[(TopicPartitionOffset, Option[Entry])],
    at: FiniteDuration
  ): ShareConsumerState =
    changes.foldLeft(this) { case (s, (p, next)) => s.updated(p, next, at) }

  private def withStatus(
    offsets: Iterable[TopicPartitionOffset],
    at: FiniteDuration
  )(status: Entry => Option[Entry]): ShareConsumerState =
    withStatuses(offsets.flatMap(p => records.get(p).map(e => p -> status(e))), at)

  /**
    * Records a chunk's acknowledgements, the last per record winning. One for a record being
    * renewed is kept until the record is back. Returns the offsets whose delivery isn't held.
    */
  def acknowledge(
    acks: Chunk[Acknowledgement],
    at: FiniteDuration
  ): (ShareConsumerState, Vector[TopicPartitionOffset]) = {
    val (inFlight, notHeld) = acks.toVector.partition(a => holds(a.offset, a.deliveryCount))
    val last = inFlight.foldLeft(Map.empty[TopicPartitionOffset, AcknowledgeType]) { (m, a) =>
      m.updated(a.offset, a.acknowledgeType)
    }
    val next = withStatuses(
      last.flatMap { case (p, kind) =>
        records.get(p).map { e =>
          p -> Some(e.copy(status = e.status match {
            case Renewing(_) => Renewing(Some(kind))
            case _ => Acked(kind)
          }))
        }
      },
      at
    )
    (next, notHeld.map(_.offset))
  }

  /**
    * Takes the acknowledgements to send: their records are no longer held.
    */
  def takeUnsent: (ShareConsumerState, Acks) = {
    val sent = unsent.toVector.flatMap(p => records.get(p).map(p -> _))
    (
      withStatuses(sent.map { case (p, _) => p -> None }, lockedSince),
      sent.collect { case (_, Entry(d, Acked(kind))) => d.record -> kind }
    )
  }

  /**
    * Acknowledges the records held for `max` without an acknowledgement with `kind`, and takes the
    * acknowledgements to send.
    */
  def takeOverdue(
    max: FiniteDuration,
    kind: AcknowledgeType,
    now: FiniteDuration
  ): (ShareConsumerState, Acks) =
    withStatus(timed.iterator.takeWhile { case (since, _) => since + max <= now }.map(_._2).toVector, now)(
      e => Some(e.copy(status = Acked(kind)))
    ).takeUnsent

  /**
    * Closes the poll once `process` returned: the records it left get `kind`, user renewals
    * included. Records being renewed get it once they are back.
    */
  def close(kind: AcknowledgeType, at: FiniteDuration): ShareConsumerState =
    copy(open = false).withStatuses(
      records.collect {
        case (p, e @ Entry(_, Renewing(kept))) if kept.forall(_ == AcknowledgeType.Renew) =>
          p -> Some(e.copy(status = Renewing(Some(kind))))
        case (p, e) if e.unacked > 0 => p -> Some(e.copy(status = Acked(kind)))
      },
      at
    )

  /**
    * Renews every unacknowledged record, restarting the time held of those the user renewed, and
    * takes the acknowledgements to send. The renewed records can't be acknowledged until a poll
    * returns them.
    */
  def takeRenewal(at: FiniteDuration): (ShareConsumerState, Renewal) = {
    val (taken, acks) = takeUnsent
    val renewed = taken.records.toVector.collect {
      case (p, e) if e.unacked > 0 =>
        val delivery = if (e.renewal > 0) e.delivery.copy(heldSince = at) else e.delivery
        p -> Entry(delivery, Renewing(None))
    }
    (
      taken.withStatuses(renewed.map { case (p, e) => p -> Some(e) }, at).copy(lockedSince = at),
      Renewal(acks, renewed.map { case (p, e) => p -> e.delivery.record })
    )
  }

  /**
    * Unsubscribed, nothing is renewed: user renewals are dropped, and the next renewal is scheduled
    * from now, or it would be due again at once.
    */
  def skipRenewal(at: FiniteDuration): ShareConsumerState = {
    val renewed =
      if (renewals > 0) records.filter(_._2.renewal > 0).keySet else Set.empty[TopicPartitionOffset]
    withStatus(renewed, at)(e => Some(e.copy(status = Unacked))).copy(lockedSince = at)
  }

  /**
    * Holds the records the broker confirmed again, with the acknowledgement made meanwhile; the
    * others are lost. Returns the returned records which are not held.
    */
  def endRenewal(
    returned: Vector[KafkaByteConsumerRecord],
    reportedLock: Option[FiniteDuration],
    at: FiniteDuration
  ): (ShareConsumerState, Vector[KafkaByteConsumerRecord]) = {
    val byOffset = returned.map(r => offsetOf(r) -> r).toMap
    val ended = records.toVector.collect { case (p, Entry(d, Renewing(kept))) =>
      p -> byOffset.get(p).map(record => Entry(d.copy(record = record), kept.fold[Status](Unacked)(Acked(_))))
    }
    (
      withStatuses(ended, at).copy(lock = reportedLock.orElse(lock)),
      returned.filterNot(r => records.contains(offsetOf(r)))
    )
  }

  /**
    * Holds the records being renewed again as they were.
    */
  def failRenewal(at: FiniteDuration): ShareConsumerState =
    endRenewal(records.values.collect { case Entry(d, Renewing(_)) => d.record }.toVector, None, at)._1

}

private[kafka] object ShareConsumerState {

  /**
    * A record's status: unacknowledged, acknowledged but not sent (a `Renew` waiting for the next
    * renewal), or being renewed, with the acknowledgement made meanwhile.
    */
  sealed abstract class Status
  case object Unacked extends Status
  final case class Acked(kind: AcknowledgeType) extends Status
  final case class Renewing(kept: Option[AcknowledgeType]) extends Status

  final case class Entry(delivery: Delivery, status: Status) {

    def unacked: Int =
      status match {
        case Unacked | Acked(AcknowledgeType.Renew) => 1
        case _ => 0
      }

    def unsent: Int =
      status match {
        case Acked(kind) if kind != AcknowledgeType.Renew => 1
        case _ => 0
      }

    def renewal: Int =
      status match {
        case Acked(AcknowledgeType.Renew) => 1
        case _ => 0
      }

    /**
      * A record the user renewed is not timed: its time restarts with the renewal.
      */
    def timed(offset: TopicPartitionOffset): Option[(FiniteDuration, TopicPartitionOffset)] =
      if (status == Unacked) Some(delivery.heldSince -> offset) else None

  }

  def offsetOf(record: KafkaByteConsumerRecord): TopicPartitionOffset =
    new org.apache.kafka.common.TopicPartition(record.topic, record.partition) -> record.offset

  val empty: ShareConsumerState =
    ShareConsumerState(false, Map.empty, 0, Set.empty, 0, None, SortedSet.empty, Duration.Zero, None)

  type Acks = Vector[(KafkaByteConsumerRecord, AcknowledgeType)]

  /**
    * What a renewal gives Java: the acknowledgements to send, and the records to renew.
    */
  final case class Renewal(acks: Acks, records: Vector[(TopicPartitionOffset, KafkaByteConsumerRecord)])

}
