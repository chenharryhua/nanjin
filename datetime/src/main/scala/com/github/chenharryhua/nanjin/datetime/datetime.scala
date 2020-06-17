package com.github.chenharryhua.nanjin

import java.time.ZoneId

package object datetime extends DateTimeInstances {
  object iso extends IsoDateTimeInstance
  val today: NJDateTimeRange     = NJDateTimeRange.today
  val yesterday: NJDateTimeRange = NJDateTimeRange.yesterday

  val utcTime: ZoneId = ZoneId.of("Etc/UTC")

  val melbourneTime: ZoneId = ZoneId.of("Australia/Melbourne")
  val sydneyTime: ZoneId    = ZoneId.of("Australia/Sydney")
  val beijingTime: ZoneId   = ZoneId.of("Asia/Shanghai")
  val newyorkTime: ZoneId   = ZoneId.of("America/New_York")
  val londonTime: ZoneId    = ZoneId.of("Europe/London")
  val berlinTime: ZoneId    = ZoneId.of("Europe/Berlin")
}
