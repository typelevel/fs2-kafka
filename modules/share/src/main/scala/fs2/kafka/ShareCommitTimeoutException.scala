/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.errors.TimeoutException
import scala.concurrent.duration.FiniteDuration

/**
  * [[ShareCommitTimeoutException]] indicates that the broker did not confirm a share consumer's
  * commit within its commit timeout, like [[CommitTimeoutException]] for `KafkaConsumer`. The
  * pending acknowledgements may still have been applied.
  */
sealed abstract class ShareCommitTimeoutException(timeout: FiniteDuration, cause: Throwable)
  extends TimeoutException(s"share commit timeout after $timeout") {

  initCause(cause)

  override def toString: String =
    s"fs2.kafka.ShareCommitTimeoutException: $getMessage"

}

private[kafka] object ShareCommitTimeoutException {

  def apply(timeout: FiniteDuration, cause: Throwable): ShareCommitTimeoutException =
    new ShareCommitTimeoutException(timeout, cause) {}

}
