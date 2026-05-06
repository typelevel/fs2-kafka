/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import org.apache.kafka.clients.CommonClientConfigs

final private[kafka] case class ProducerMetadata(
  clientId: Option[String]
)

private[kafka] object ProducerMetadata {

  def fromProperties(properties: Map[String, String]): ProducerMetadata =
    ProducerMetadata(
      properties.get(CommonClientConfigs.CLIENT_ID_CONFIG)
    )

}
