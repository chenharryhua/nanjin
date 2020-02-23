package com.github.chenharryhua.nanjin

package object datetime extends DateTimeInstances {
  object iso extends IsoDateTimeInstance
  val today: NJDateTimeRange     = NJDateTimeRange.today
  val yesterday: NJDateTimeRange = NJDateTimeRange.yesterday
}
