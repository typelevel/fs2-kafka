/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

/**
  * The available options for [[SchemaRegistryClientSettings#withAuth]].<br><br>
  *
  * Available options include:<br>
  *   - [[Auth.Basic]] to authenticate with username and password,<br>
  *   - [[Auth.Bearer]] to authenticate with an authentication token,<br>
  *   - [[Auth.None]] to not provide any authentication details.
  */
sealed abstract class Auth

object Auth {

  final private[vulcan] case class BasicAuth(username: String, password: String) extends Auth {
    override def toString: String = s"Basic($username)"
  }

  final private[vulcan] case class BearerAuth(token: String) extends Auth {
    override def toString: String = "Bearer"
  }

  private[vulcan] case object NoAuth extends Auth {
    override def toString: String = "None"
  }

  /**
    * Option to authenticate with username and password.
    */
  def Basic(username: String, password: String): Auth =
    BasicAuth(username, password)

  /**
    * Option to authenticate with an authentication token.
    */
  def Bearer(token: String): Auth =
    BearerAuth(token)

  /**
    * Option to not provide any authentication details.
    */
  val None: Auth =
    NoAuth

}
