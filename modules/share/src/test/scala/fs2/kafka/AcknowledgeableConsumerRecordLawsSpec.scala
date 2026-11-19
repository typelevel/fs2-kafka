/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.BitraverseTests
import cats.laws.discipline.TraverseTests

class AcknowledgeableConsumerRecordLawsSpec extends BaseCatsSpec with ShareGenerators {

  checkAll(
    "AcknowledgeableConsumerRecord.eqLaws",
    EqTests[AcknowledgeableConsumerRecord[IO, String, String]].eqv
  )

  checkAll(
    "AcknowledgeableConsumerRecord.traverseLaws",
    TraverseTests[AcknowledgeableConsumerRecord[IO, Int, *]].traverse[Int, Int, Int, Set[
      Int
    ], Option, Option]
  )

  checkAll(
    "AcknowledgeableConsumerRecord.bitraverseLaws",
    BitraverseTests[AcknowledgeableConsumerRecord[IO, *, *]].bitraverse[
      Option,
      Int,
      Int,
      Int,
      String,
      String,
      String
    ]
  )

}
