/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

trait KafkaConsumerLifecycle[F[_]] {

  /**
    * Whenever `terminate` is invoked, an attempt will be made to stop the underlying consumer. The
    * `terminate` operation will not wait for the consumer to shutdown. If you also want to wait for
    * the shutdown to complete, you can use `terminate >> awaitTermination`.
    */
  def terminate: F[Unit]

  /**
    * Wait for consumer to shut down. Note that `awaitTermination` is guaranteed to complete after
    * consumer shutdown, even when the consumer is cancelled with `terminate`.<br><br>
    *
    * This method will not initiate shutdown. To initiate shutdown and wait for it to complete, you
    * can use `terminate >> awaitTermination`.
    */
  def awaitTermination: F[Unit]

}
