package com.github.chenharryhua.nanjin.common

import java.time.LocalDateTime

object initial {

  final val epoch: LocalDateTime = LocalDateTime.of(2019, 7, 21, 0, 0, 0)

  // kafka was graduated from apache incubator
  final val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)
  final val sparkEpoch: LocalDateTime = LocalDateTime.of(2014, 2, 1, 0, 0, 0)
  final val flinkEpoch: LocalDateTime = LocalDateTime.of(2014, 12, 1, 0, 0, 0)
}
