package com.github.chenharryhua.nanjin.spark.dstream

import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint

final case class DStreamParams()

@deriveFixedPoint trait DStreamConfigF[_]

object DStreamConfigF {
  final case class DefaultParams[K]() extends DStreamConfigF[K]
}

final case class DStreamConfig(value: Fix[DStreamConfigF])

object DStreamConfig {

  def apply(): DStreamConfig =
    new DStreamConfig(Fix(DStreamConfigF.DefaultParams[Fix[DStreamConfigF]]()))
}
