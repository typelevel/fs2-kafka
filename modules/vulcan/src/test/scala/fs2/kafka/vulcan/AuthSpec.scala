/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.*

final class AuthSpec extends AnyFunSpec with ScalaCheckPropertyChecks {

  describe("Auth.Basic") {
    it("should include the username in toString") {
      forAll { (username: String, password: String) =>
        val auth = Auth.Basic(username, password)
        assert(auth.toString.contains(username))
      }
    }

    it("should not include the password in toString") {
      forAll { (username: String, password: String) =>
        whenever(!username.contains(password)) {
          val auth = Auth.Basic(username, password)
          assert(!auth.toString.contains(password))
        }
      }
    }
  }

  describe("Auth.Bearer") {
    it("should not include the token in toString") {
      forAll { (token: String) =>
        whenever(!"Bearer".contains(token)) {
          val auth = Auth.Bearer(token)
          assert(!auth.toString.contains(token))
        }
      }
    }
  }

  describe("Auth.None") {
    it("should have None as toString") {
      assert(Auth.None.toString == "None")
    }
  }

}
