/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.{BitraverseTests, TraverseTests}

class ProducerRecordLawsSpec extends BaseCatsSpec {

  checkAll(
    "ProducerRecord.eqLaws",
    EqTests[ProducerRecord[Int, Int]].eqv
  )

  checkAll(
    "ProducerRecord.traverseLaws",
    TraverseTests[ProducerRecord[Int, *]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )

  checkAll(
    "ProducerRecord.bitraverseLaws",
    BitraverseTests[ProducerRecord].bitraverse[Option, Int, Int, Int, String, String, String]
  )

}
