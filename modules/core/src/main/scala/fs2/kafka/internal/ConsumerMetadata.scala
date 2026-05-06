/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import org.apache.kafka.clients.CommonClientConfigs

final private[kafka] case class ConsumerMetadata(
  clientId: Option[String],
  groupId: Option[String]
)

private[kafka] object ConsumerMetadata {

  def fromProperties(properties: Map[String, String]): ConsumerMetadata =
    ConsumerMetadata(
      properties.get(CommonClientConfigs.CLIENT_ID_CONFIG),
      properties.get(CommonClientConfigs.GROUP_ID_CONFIG)
    )

}
