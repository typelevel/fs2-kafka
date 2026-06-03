/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lenses.k2k.it;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public final class AttemptTest2 {

  private AttemptTest2() {}

  public static final class KafkaConsumerStandIn {

    private KafkaConsumerStandIn() {}

    /**
     * Builds a {@link KafkaConsumer} from the given client configuration map (e.g. {@code
     * bootstrap.servers}, deserializers, group id).
     */
    public static <K, V> KafkaConsumer<K, V> build(Map<String, Object> config) {
      Properties props = new Properties();
      props.putAll(config);
      return new KafkaConsumer<>(props);
    }
  }
}
