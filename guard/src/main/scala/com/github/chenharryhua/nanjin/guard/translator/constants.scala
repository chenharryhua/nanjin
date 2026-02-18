package com.github.chenharryhua.nanjin.guard.translator

import com.codahale.metrics.MetricAttribute

object metricConstants {
  // counters
  @inline final val METRICS_COUNT: String = MetricAttribute.COUNT.getCode

  // meters
  @inline final val METRICS_MEAN_RATE: String = MetricAttribute.MEAN_RATE.getCode
  @inline final val METRICS_1_MINUTE_RATE: String = MetricAttribute.M1_RATE.getCode
  @inline final val METRICS_5_MINUTE_RATE: String = MetricAttribute.M5_RATE.getCode
  @inline final val METRICS_15_MINUTE_RATE: String = MetricAttribute.M15_RATE.getCode

  // histograms
  @inline final val METRICS_MIN: String = MetricAttribute.MIN.getCode
  @inline final val METRICS_MAX: String = MetricAttribute.MAX.getCode
  @inline final val METRICS_MEAN: String = MetricAttribute.MEAN.getCode
  @inline final val METRICS_STD_DEV: String = MetricAttribute.STDDEV.getCode

  @inline final val METRICS_P50: String = MetricAttribute.P50.getCode
  @inline final val METRICS_P75: String = MetricAttribute.P75.getCode
  @inline final val METRICS_P95: String = MetricAttribute.P95.getCode
  @inline final val METRICS_P98: String = MetricAttribute.P98.getCode
  @inline final val METRICS_P99: String = MetricAttribute.P99.getCode
  @inline final val METRICS_P999: String = MetricAttribute.P999.getCode
}

object textConstants {
  @inline final val CONSTANT_METRICS: String = "Metrics"
  @inline final val CONSTANT_SNOOZED: String = "Snoozed"
  @inline final val CONSTANT_ACTIVE: String = "Active"
}
