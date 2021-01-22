package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import higherkindness.droste.data.Fix
import monocle.macros.Lenses

import java.time.ZoneId

@Lenses final private[dstream] case class SDParams private (
  zoneId: ZoneId,
  pathBuilder: NJTimestamp => String
)

sealed private[dstream] trait SDConfigF[A]

private[dstream] object SDConfigF {}

final private[dstream] case class SDConfig private (value: Fix[SDConfigF]) {}
