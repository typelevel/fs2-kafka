/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.{BitraverseTests, TraverseTests}

class ConsumerRecordLawsSpec extends BaseCatsSpec {

  checkAll(
    "ConsumerRecord.eqLaws",
    EqTests[ConsumerRecord[Int, Int]].eqv
  )

  checkAll(
    "ConsumerRecord.traverseLaws",
    TraverseTests[ConsumerRecord[Int, *]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )

  checkAll(
    "ConsumerRecord.bitraverseLaws",
    BitraverseTests[ConsumerRecord].bitraverse[Option, Int, Int, Int, String, String, String]
  )

}
