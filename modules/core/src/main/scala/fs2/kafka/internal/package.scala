/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

package object internal {
  private[kafka] type ExclusiveAccess[F[_], A] = F[A] => F[A]
}
