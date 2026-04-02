/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import scala.collection.immutable.ArraySeq

private[kafka] object converters {

  val collection = scala.jdk.CollectionConverters
  val option     = scala.jdk.OptionConverters
  val duration   = scala.jdk.DurationConverters

  def unsafeWrapArray[A](array: Array[A]): Seq[A] =
    ArraySeq.unsafeWrapArray(array)

}
