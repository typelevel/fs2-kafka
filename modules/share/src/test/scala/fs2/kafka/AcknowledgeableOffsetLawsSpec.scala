/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.kernel.laws.discipline.EqTests

class AcknowledgeableOffsetLawsSpec extends BaseCatsSpec with ShareGenerators {

  checkAll(
    "AcknowledgeableOffset.eqLaws",
    EqTests[AcknowledgeableOffset[IO]].eqv
  )

}
