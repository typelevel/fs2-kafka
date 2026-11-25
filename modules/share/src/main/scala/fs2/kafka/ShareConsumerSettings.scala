/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import cats.effect.Resource
import fs2.concurrent.Signal
import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.concurrent.duration.*

/**
  * [[ShareConsumerSettings]] contain settings necessary to create a [[KafkaShareConsumer]]. At the
  * very least, this includes key and value deserializers.<br><br>
  *
  * Unlike Java's share consumer, these settings default to `explicit` acknowledgement and
  * `record_limit` acquisition, which can be changed with [[withAcknowledgementMode]] and
  * [[withAcquireMode]]. As in Java, records are only renewed automatically with
  * [[withAutoRenewal]].<br><br>
  *
  * Several convenience functions are provided so that you don't have to work with `String` values
  * and `ConsumerConfig` for configuration. It's still possible to specify `ConsumerConfig` values
  * with functions like [[withProperty]].<br><br>
  *
  * [[ShareConsumerSettings]] instances are immutable and all modification functions return a new
  * [[ShareConsumerSettings]] instance. Use `ShareConsumerSettings#apply` to create a new instance.
  *
  * @note
  *   this API is experimental, as are Kafka's share groups.
  */
sealed abstract class ShareConsumerSettings[F[_], K, V] {

  /**
    * The `Deserializer` to use for deserializing record keys.
    */
  def keyDeserializer: Resource[F, KeyDeserializer[F, K]]

  /**
    * The `Deserializer` to use for deserializing record values.
    */
  def valueDeserializer: Resource[F, ValueDeserializer[F, V]]

  /**
    * Creates a new [[ShareConsumerSettings]] instance that replaces the deserializers with those
    * provided.
    */
  def withDeserializers[K0, V0](
    keyDeserializer: Resource[F, KeyDeserializer[F, K0]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V0]]
  ): ShareConsumerSettings[F, K0, V0]

  /**
    * Properties which can be provided when creating a Java `KafkaShareConsumer` instance. Numerous
    * functions in [[ShareConsumerSettings]] add properties here if the settings are used by the
    * Java `KafkaShareConsumer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified bootstrap servers. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  final def withBootstrapServers(servers: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified client id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  final def withClientId(clientId: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified share group id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.GROUP_ID_CONFIG
    * }}}
    */
  final def withGroupId(groupId: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified max poll records. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with an `Int` instead of a `String`. With `record_limit` acquisition, the
    * default, it is also the most records a consumer holds at once.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_RECORDS_CONFIG
    * }}}
    */
  final def withMaxPollRecords(maxPollRecords: Int): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified acknowledgement mode. This
    * is equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG
    * }}}
    */
  final def withAcknowledgementMode(
    mode: ShareAcknowledgementMode
  ): ShareConsumerSettings[F, K, V] =
    withProperty(
      ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
      mode match {
        case ShareAcknowledgementMode.ExplicitAcknowledgement => "explicit"
        case ShareAcknowledgementMode.ImplicitAcknowledgement => "implicit"
      }
    )

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified acquire mode. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG
    * }}}
    */
  final def withAcquireMode(mode: ShareAcquireMode): ShareConsumerSettings[F, K, V] =
    withProperty(
      ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG,
      mode match {
        case ShareAcquireMode.RecordLimitAcquire => "record_limit"
        case ShareAcquireMode.BatchOptimizedAcquire => "batch_optimized"
      }
    )

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified max poll interval. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
    * }}}
    */
  final def withMaxPollInterval(interval: FiniteDuration): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, interval.toMillis.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified request timeout. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  final def withRequestTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified default api timeout. This
    * is equivalent to setting the following property using the [[withProperty]] function, except
    * you can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
    * }}}
    */
  final def withDefaultApiTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified allow auto create topics.
    * This is equivalent to setting the following property using the [[withProperty]] function,
    * except you can specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG
    * }}}
    */
  final def withAllowAutoCreateTopics(allow: Boolean): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allow.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified client rack. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_RACK_CONFIG
    * }}}
    */
  final def withClientRack(rack: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.CLIENT_RACK_CONFIG, rack)

  /**
    * Includes a property with the specified `key` and `value`. The key should be one of the keys in
    * `ConsumerConfig`, and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ShareConsumerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  final def withProperties(properties: (String, String)*): ShareConsumerSettings[F, K, V] =
    withProperties(properties.toMap)

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ShareConsumerSettings[F, K, V]

  /**
    * Includes the credentials properties from the provided
    * [[fs2.kafka.security.KafkaCredentialStore]].
    */
  final def withCredentials(credentialsStore: KafkaCredentialStore): ShareConsumerSettings[F, K, V] =
    withProperties(credentialsStore.properties)

  /**
    * The time to wait for the Java `KafkaShareConsumer` to shutdown.<br><br>
    *
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * How long stopping waits for the broker to confirm the last acknowledgements, before failing
    * `consume` with a [[ShareCommitTimeoutException]]. It also bounds the wait for a pending
    * renewal when `consume` ends, and the commit before `unsubscribe`.<br><br>
    *
    * The default value is 15 seconds.
    */
  def commitTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[commitTimeout]].
    */
  def withCommitTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * How long we should allow the `poll` call to block for in the Java `KafkaShareConsumer`, which
    * is also how long each poll of an idle consumer waits for records.<br><br>
    *
    * The default value is 50 milliseconds.
    */
  def pollTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[pollTimeout]].
    */
  def withPollTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * Whether records without an acknowledgement are renewed automatically, [[renewalMargin]] before
    * their acquisition lock ends. It applies with explicit acknowledgement.<br><br>
    *
    * The default value is `false`, as in Java's share consumer.
    */
  def autoRenewal: Boolean

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[autoRenewal]].<br><br>
    *
    * Enable it when processing may outlast the lock: without it, a record neither acknowledged nor
    * renewed in time is delivered again. A hung record is then renewed until the
    * [[maxProcessingTime]] passes.
    */
  def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V]

  /**
    * How long before a record's lock ends [[autoRenewal]] renews it, and at least every half of
    * `max.poll.interval.ms`. A margin of the whole lock or more renews at half the lock. The lock
    * counts from the start of the poll, so with `batch_optimized` acquisition, a record Java
    * buffered for longer than the margin can lose its lock first.<br><br>
    *
    * The default value is 5 seconds.
    */
  def renewalMargin: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[renewalMargin]].
    */
  def withRenewalMargin(margin: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * How long the consumer may hold a record without an acknowledgement, or `None` for no limit. A
    * record held longer gets the [[failureAcknowledgement]]; its processing goes on, and its later
    * acknowledgement fails with a [[RecordNotInFlightException]], which [[acknowledgeChunks]]
    * skips. It applies with explicit acknowledgement.<br><br>
    *
    * The time counts from the poll, or from the user's latest `Renew` of the record, so set it well
    * above the time to process a whole poll. Ignoring the failure is usually enough, as the record
    * was released and is delivered again:
    *
    * {{{
    * consumer.consume(_.parTraverseN(8) { record =>
    *   process(record.record) *>
    *     record.accept.recover { case _: RecordNotInFlightException => () }
    * }.void)
    * }}}
    *
    * The default value is `None`.
    */
  def maxProcessingTime: Option[FiniteDuration]

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[maxProcessingTime]], at most 365
    * days. For no limit, use [[withoutMaxProcessingTime]].
    */
  def withMaxProcessingTime(time: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * Creates a new [[ShareConsumerSettings]] without a [[maxProcessingTime]], the default.
    */
  def withoutMaxProcessingTime: ShareConsumerSettings[F, K, V]

  /**
    * How the consumer acknowledges the records it settles itself: those `process` leaves
    * unacknowledged when it returns or fails, those of a poll in which a record fails to decode,
    * and those past the [[maxProcessingTime]]. `Release` delivers them again, `Reject` does not.
    * Records of a canceled `process` are always released. It applies with explicit
    * acknowledgement.<br><br>
    *
    * A failing `process` or decoder settles every unacknowledged record of its poll, so with
    * `Reject`, handle failures per record, for example with `attempt` deserializers, rather than
    * failing `process`.<br><br>
    *
    * The default value is `Release`, so these records are delivered again.
    */
  def failureAcknowledgement: AcknowledgeType

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[failureAcknowledgement]], which
    * must be `Release` or `Reject`.
    */
  def withFailureAcknowledgement(kind: AcknowledgeType): ShareConsumerSettings[F, K, V]

  /**
    * How long a pending acknowledgement may wait before the consumer sends it, without waiting for
    * the result, or `None` to send acknowledgements only with the next poll, while paused, and when
    * consuming ends, as a Java loop does. Without a limit, acknowledgements wait for the slowest
    * record of the poll, and can be lost with their lock. It applies with explicit
    * acknowledgement.<br><br>
    *
    * The default value is 1 second.
    */
  def maxAcknowledgementDelay: Option[FiniteDuration]

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[maxAcknowledgementDelay]], at
    * most 365 days. For no limit, use [[withoutMaxAcknowledgementDelay]].
    */
  def withMaxAcknowledgementDelay(delay: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * Creates a new [[ShareConsumerSettings]] without a [[maxAcknowledgementDelay]], so that
    * acknowledgements are sent with the next poll, as in a Java loop.
    */
  def withoutMaxAcknowledgementDelay: ShareConsumerSettings[F, K, V]

  /**
    * A signal which pauses the consumer while it is `true`, or `None` to never pause. While paused,
    * no new records are polled, so their delivery count does not grow while, for example, a service
    * the processing needs is down. Records already held are acknowledged and renewed as
    * usual.<br><br>
    *
    * If the last poll before the pause returned nothing, Java may still acquire records once; those
    * are delivered after the pause with a higher delivery count. Without records held, a pause
    * longer than `max.poll.interval.ms` makes Java leave the share group until the next
    * poll.<br><br>
    *
    * The default value is `None`.
    */
  def pauseWhen: Option[Signal[F, Boolean]]

  /**
    * Creates a new [[ShareConsumerSettings]] which pauses while the signal is `true`, see
    * [[pauseWhen]].
    */
  def withPauseWhen(signal: Signal[F, Boolean]): ShareConsumerSettings[F, K, V]

  /**
    * Creates a new [[ShareConsumerSettings]] which never pauses, the default.
    */
  def withoutPauseWhen: ShareConsumerSettings[F, K, V]

}

object ShareConsumerSettings {

  /**
    * The longest limit, far from overflowing the deadlines computed from it.
    */
  private val maxLimit: FiniteDuration = 365.days

  final private case class Impl[F[_], K, V](
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]],
    properties: Map[String, String],
    closeTimeout: FiniteDuration,
    pollTimeout: FiniteDuration,
    commitTimeout: FiniteDuration,
    autoRenewal: Boolean,
    renewalMargin: FiniteDuration,
    maxProcessingTime: Option[FiniteDuration],
    failureAcknowledgement: AcknowledgeType,
    maxAcknowledgementDelay: Option[FiniteDuration],
    pauseWhen: Option[Signal[F, Boolean]]
  ) extends ShareConsumerSettings[F, K, V] {

    override def withProperty(key: String, value: String): ShareConsumerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(values: Map[String, String]): ShareConsumerSettings[F, K, V] =
      copy(properties = properties ++ values)

    override def withDeserializers[K0, V0](
      keyDeserializer: Resource[F, KeyDeserializer[F, K0]],
      valueDeserializer: Resource[F, ValueDeserializer[F, V0]]
    ): ShareConsumerSettings[F, K0, V0] =
      copy(keyDeserializer = keyDeserializer, valueDeserializer = valueDeserializer)

    override def withCloseTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout >= Duration.Zero, "close timeout must be non-negative")
      copy(closeTimeout = timeout)
    }

    override def withPollTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout > Duration.Zero, "poll timeout must be positive")
      copy(pollTimeout = timeout)
    }

    override def withCommitTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout > Duration.Zero, "commit timeout must be positive")
      copy(commitTimeout = timeout)
    }

    override def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V] =
      copy(autoRenewal = enabled)

    override def withRenewalMargin(margin: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(margin > Duration.Zero, "renewal margin must be positive")
      copy(renewalMargin = margin)
    }

    override def withMaxProcessingTime(time: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(time > Duration.Zero, "max processing time must be positive")
      require(
        time <= maxLimit,
        "max processing time must be at most 365 days; use withoutMaxProcessingTime for no limit"
      )
      copy(maxProcessingTime = Some(time))
    }

    override def withoutMaxProcessingTime: ShareConsumerSettings[F, K, V] =
      copy(maxProcessingTime = None)

    override def withFailureAcknowledgement(kind: AcknowledgeType): ShareConsumerSettings[F, K, V] = {
      require(
        kind == AcknowledgeType.Release || kind == AcknowledgeType.Reject,
        "failure acknowledgement must be Release or Reject"
      )
      copy(failureAcknowledgement = kind)
    }

    override def withMaxAcknowledgementDelay(
      delay: FiniteDuration
    ): ShareConsumerSettings[F, K, V] = {
      require(delay >= Duration.Zero, "max acknowledgement delay must be non-negative")
      require(
        delay <= maxLimit,
        "max acknowledgement delay must be at most 365 days; use withoutMaxAcknowledgementDelay for no limit"
      )
      copy(maxAcknowledgementDelay = Some(delay))
    }

    override def withoutMaxAcknowledgementDelay: ShareConsumerSettings[F, K, V] =
      copy(maxAcknowledgementDelay = None)

    override def withPauseWhen(signal: Signal[F, Boolean]): ShareConsumerSettings[F, K, V] =
      copy(pauseWhen = Some(signal))

    override def withoutPauseWhen: ShareConsumerSettings[F, K, V] =
      copy(pauseWhen = None)

    override def toString: String =
      s"ShareConsumerSettings(closeTimeout = $closeTimeout, pollTimeout = $pollTimeout, commitTimeout = $commitTimeout, autoRenewal = $autoRenewal, renewalMargin = $renewalMargin, maxProcessingTime = $maxProcessingTime, failureAcknowledgement = $failureAcknowledgement, maxAcknowledgementDelay = $maxAcknowledgementDelay, pauseWhen = ${
          if (pauseWhen.isDefined) "Some(<signal>)"
          else "None"
        })"

  }

  def apply[F[_], K, V](
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V]
  ): ShareConsumerSettings[F, K, V] =
    apply(
      Resource.pure[F, KeyDeserializer[F, K]](keyDeserializer),
      Resource.pure[F, ValueDeserializer[F, V]](valueDeserializer)
    )

  def apply[F[_], K, V](implicit
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]]
  ): ShareConsumerSettings[F, K, V] =
    Impl(
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer,
      properties = Map.empty,
      closeTimeout = 20.seconds,
      pollTimeout = 50.millis,
      commitTimeout = 15.seconds,
      autoRenewal = false,
      renewalMargin = 5.seconds,
      maxProcessingTime = None,
      failureAcknowledgement = AcknowledgeType.Release,
      maxAcknowledgementDelay = Some(1.second),
      pauseWhen = None
    ).withAcknowledgementMode(ShareAcknowledgementMode.Explicit)
      .withAcquireMode(ShareAcquireMode.RecordLimit)

  implicit def shareConsumerSettingsShow[F[_], K, V]: Show[ShareConsumerSettings[F, K, V]] =
    Show.fromToString

}
