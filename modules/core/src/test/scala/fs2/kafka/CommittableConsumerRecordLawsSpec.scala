/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.{BitraverseTests, TraverseTests}

class CommittableConsumerRecordLawsSpec extends BaseCatsSpec {

  checkAll(
    "CommittableConsumerRecord.eqLaws",
    EqTests[CommittableConsumerRecord[IO, String, String]].eqv
  )

  checkAll(
    "CommittableConsumerRecord.traverseLaws",
    TraverseTests[CommittableConsumerRecord[IO, Int, *]].traverse[Int, Int, Int, Set[
      Int
    ], Option, Option]
  )

  checkAll(
    "CommittableConsumerRecord.bitraverseLaws",
    BitraverseTests[CommittableConsumerRecord[IO, *, *]].bitraverse[
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
