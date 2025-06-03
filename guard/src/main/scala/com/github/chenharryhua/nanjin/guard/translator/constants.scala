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
  @inline final val CONSTANT_TIMESTAMP: String = "Timestamp"
  @inline final val CONSTANT_POLICY: String = "Policy"
  @inline final val CONSTANT_CAUSE: String = "Cause"
  @inline final val CONSTANT_TOOK: String = "Took"
  @inline final val CONSTANT_INDEX: String = "Index"
  @inline final val CONSTANT_UPTIME: String = "UpTime"
  @inline final val CONSTANT_BRIEF: String = "Brief"
  @inline final val CONSTANT_METRICS: String = "Metrics"
  @inline final val CONSTANT_TIMEZONE: String = "TimeZone"
  @inline final val CONSTANT_SERVICE: String = "Service"
  @inline final val CONSTANT_SERVICE_ID: String = "ServiceID"
  @inline final val CONSTANT_HOST: String = "Host"
  @inline final val CONSTANT_TASK: String = "Task"
  @inline final val CONSTANT_DOMAIN: String = "Domain"
  @inline final val CONSTANT_SNOOZED: String = "Snoozed"
  @inline final val CONSTANT_ACTIVE: String = "Active"
  @inline final val CONSTANT_ALARM_LEVEL: String = "Level"
  @inline final val CONSTANT_MESSAGE_TOKEN: String = "Token"

  @inline final val CONSTANT_LABEL: String = "Label"
  @inline final val CONSTANT_LAUNCH_TIME: String = "LaunchTime"

}
