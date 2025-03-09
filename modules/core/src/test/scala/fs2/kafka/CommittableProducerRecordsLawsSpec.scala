/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.{BitraverseTests, TraverseTests}

class CommittableProducerRecordsLawsSpec extends BaseCatsSpec {

  checkAll(
    "CommittableProducerRecord.eqLaws",
    EqTests[CommittableProducerRecords[IO, String, String]].eqv
  )

  checkAll(
    "CommittableProducerRecord.traverseLaws",
    TraverseTests[CommittableProducerRecords[IO, Int, *]].traverse[Int, Int, Int, Set[
      Int
    ], Option, Option]
  )

  checkAll(
    "CommittableProducerRecord.bitraverseLaws",
    BitraverseTests[CommittableProducerRecords[IO, *, *]].bitraverse[
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
