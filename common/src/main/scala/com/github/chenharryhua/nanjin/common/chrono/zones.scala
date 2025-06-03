package com.github.chenharryhua.nanjin.common.chrono

import java.time.ZoneId

object zones {
  final val utcTime: ZoneId = ZoneId.of("Etc/UTC")
  final val darwinTime: ZoneId = ZoneId.of("Australia/Darwin")
  final val sydneyTime: ZoneId = ZoneId.of("Australia/Sydney")
  final val beijingTime: ZoneId = ZoneId.of("Asia/Shanghai")
  final val singaporeTime: ZoneId = ZoneId.of("Asia/Singapore")
  final val mumbaiTime: ZoneId = ZoneId.of("Asia/Kolkata")
  final val newyorkTime: ZoneId = ZoneId.of("America/New_York")
  final val londonTime: ZoneId = ZoneId.of("Europe/London")
  final val berlinTime: ZoneId = ZoneId.of("Europe/Berlin")
  final val cairoTime: ZoneId = ZoneId.of("Africa/Cairo")
  final val saltaTime: ZoneId = ZoneId.of("America/Argentina/Salta")
}
