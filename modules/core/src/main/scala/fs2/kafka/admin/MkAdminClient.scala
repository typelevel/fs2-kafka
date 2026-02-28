/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.admin

import cats.effect.Sync
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.AdminClientSettings

import org.apache.kafka.clients.admin.AdminClient

/**
  * A capability trait representing the ability to instantiate the Java `AdminClient` that underlies
  * the fs2-kafka `KafkaAdminClient`. This is needed in order to instantiate
  * [[fs2.kafka.KafkaAdminClient]].<br><br>
  *
  * By default, the instance provided by [[MkAdminClient.mkAdminClientForSync]] will be used.
  * However this behaviour can be overridden, e.g. for testing purposes, by placing an alternative
  * implicit instance in lexical scope.
  */
trait MkAdminClient[F[_]] {
  def apply(settings: AdminClientSettings): F[AdminClient]
}

object MkAdminClient {

  implicit def mkAdminClientForSync[F[_]](implicit F: Sync[F]): MkAdminClient[F] =
    settings =>
      F.blocking {
        AdminClient.create {
          (settings.properties: Map[String, AnyRef]).asJava
        }
      }

}
