/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.util.Optional

private[kafka] object converters {

  val collection = scala.collection.JavaConverters

  object option {

    implicit class OptionOps[A](private val self: Option[A]) extends AnyVal {
      def toJava: Optional[A] = self.fold[Optional[A]](Optional.empty())(Optional.of)
    }

  }

  def unsafeWrapArray[A](array: Array[A]): Seq[A] =
    array

}
