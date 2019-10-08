package com.github.chenharryhua.nanjin.kafka

import monocle.macros.Lenses

import scala.concurrent.duration._

@Lenses case class SparkafkaConfiguration(
  timeRange: KafkaDateTimeRange,
  batchSize: Int,
  isIntact: Boolean,
  uploadRate: FiniteDuration)

object SparkafkaConfiguration {

  val default: SparkafkaConfiguration =
    SparkafkaConfiguration(KafkaDateTimeRange(None, None), 1000, isIntact = true, 1.seconds)
}
