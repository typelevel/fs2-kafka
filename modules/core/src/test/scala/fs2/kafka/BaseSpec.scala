/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.scalatest.*
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.*

abstract class BaseSpec
    extends AnyFunSpec
    with Assertions
    with Matchers
    with ScalaCheckPropertyChecks
    with BaseGenerators
