/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.FiniteDuration

import cats.Show

import org.apache.kafka.clients.producer.ProducerConfig

/**
  * [[TransactionalProducerSettings]] contain settings necessary to create a
  * [[TransactionalKafkaProducer]]. This includes a transactional ID and any other
  * [[ProducerSettings]].<br><br>
  *
  * [[TransactionalProducerSettings]] instances are immutable and modification functions return a
  * new [[TransactionalProducerSettings]] instance.<br><br>
  *
  * Use [[TransactionalProducerSettings.apply]] to create a new instance.
  */
sealed abstract class TransactionalProducerSettings[F[_], K, V] {

  /**
    * The transactional ID which should be used in transactions. This is the value for the following
    * producer property.
    *
    * {{{
    * ProducerConfig.TRANSACTIONAL_ID_CONFIG
    * }}}
    */
  def transactionalId: String

  /**
    * The producer settings including transactional properties, as configured by the
    * [[TransactionalProducerSettings]].
    */
  def producerSettings: ProducerSettings[F, K, V]

  /**
    * Returns a new [[TransactionalProducerSettings]] instance with the specified transaction
    * timeout. This is setting the following producer property, except you can specify it with a
    * `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
    * }}}
    */
  def withTransactionTimeout(
    transactionTimeout: FiniteDuration
  ): TransactionalProducerSettings[F, K, V]

}

object TransactionalProducerSettings {

  final private[this] case class TransactionalProducerSettingsImpl[F[_], K, V](
    override val transactionalId: String,
    override val producerSettings: ProducerSettings[F, K, V]
  ) extends TransactionalProducerSettings[F, K, V] {

    override def withTransactionTimeout(
      transactionTimeout: FiniteDuration
    ): TransactionalProducerSettings[F, K, V] =
      copy(
        producerSettings = producerSettings.withProperty(
          ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
          transactionTimeout.toMillis.toString
        )
      )

    override def toString: String =
      s"TransactionalProducerSettings(transactionalId = $transactionalId, producerSettings = $producerSettings)"

  }

  def apply[F[_], K, V](
    transactionalId: String,
    producerSettings: ProducerSettings[F, K, V]
  ): TransactionalProducerSettings[F, K, V] =
    TransactionalProducerSettingsImpl(
      transactionalId = transactionalId,
      producerSettings =
        producerSettings.withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    )

  implicit def transactionalProducerSettingsShow[F[_], K, V]
    : Show[TransactionalProducerSettings[F, K, V]] =
    Show.fromToString

}
