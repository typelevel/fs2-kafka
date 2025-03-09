/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.syntax.all.*

import org.apache.kafka.clients.admin.AdminClientConfig

final class AdminClientSettingsSpec extends BaseSpec {

  describe("AdminClientSettings") {
    it("should provide withBootstrapServers") {
      assert {
        settings
          .withBootstrapServers("localhost")
          .properties(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
          .contains("localhost")
      }
    }

    it("should provide withClientId") {
      assert {
        settings
          .withClientId("client")
          .properties(AdminClientConfig.CLIENT_ID_CONFIG)
          .contains("client")
      }
    }

    it("should provide withReconnectBackoff") {
      assert {
        settings
          .withReconnectBackoff(10.seconds)
          .properties(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withReconnectBackoffMax") {
      assert {
        settings
          .withReconnectBackoffMax(10.seconds)
          .properties(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRetryBackoff") {
      assert {
        settings
          .withRetryBackoff(10.seconds)
          .properties(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withConnectionsMaxIdle") {
      assert {
        settings
          .withConnectionsMaxIdle(10.seconds)
          .properties(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRequestTimeout") {
      assert {
        settings
          .withRequestTimeout(10.seconds)
          .properties(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withMetadataMaxAge") {
      assert {
        settings
          .withMetadataMaxAge(10.seconds)
          .properties(AdminClientConfig.METADATA_MAX_AGE_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRetries") {
      assert {
        settings.withRetries(10).properties(AdminClientConfig.RETRIES_CONFIG).contains("10")
      }
    }

    it("should provide withProperty") {
      assert {
        settings.withProperty("a", "b").properties("a").contains("b")
      }
    }

    it("should provide withProperties") {
      assert {
        settings.withProperties("a" -> "b").properties("a").contains("b") &&
        settings.withProperties(Map("a" -> "b")).properties("a").contains("b")
      }
    }

    it("should provide withCloseTimeout") {
      assert {
        settings.withCloseTimeout(10.seconds).closeTimeout == 10.seconds
      }
    }

    it("should have a Show instance and matching toString") {
      assert {
        settings.toString == "AdminClientSettings(closeTimeout = 20 seconds)" &&
        settings.show == settings.toString
      }
    }
  }

  def settings =
    AdminClientSettings("test")

}
