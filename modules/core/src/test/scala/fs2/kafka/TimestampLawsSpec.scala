/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.kernel.laws.discipline.EqTests

class TimestampLawsSpec extends BaseCatsSpec {
  checkAll(
    "Timestamp.eqLaws",
    EqTests[Timestamp].eqv
  )
}
