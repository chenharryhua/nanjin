package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricId, MetricKind}
import io.circe.{Decoder, Json}

object retrieve {
  def healthCheck(gauges: List[MetricElement.Gauge]): Map[MetricId, Boolean] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.Gauge(MetricKind.Gauge.HealthCheck, _) =>
          gg.gauge.value.asBoolean.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def gauge[A: Decoder](gauges: List[MetricElement.Gauge]): Map[MetricId, A] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.Gauge(MetricKind.Gauge.Default, _) =>
          gg.gauge.value.as[A].toOption.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def ratio(gauges: List[MetricElement.Gauge]): Map[MetricId, Json] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.Gauge(MetricKind.Gauge.Ratio, _) =>
          gg.metricId -> gg.gauge.value
      }
    }.toMap

  def counter(counters: List[MetricElement.Counter]): Map[MetricId, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.Counter(MetricKind.Counter.Default) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def riskCounter(counters: List[MetricElement.Counter]): Map[MetricId, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.Counter(MetricKind.Counter.Risk) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def timer(timers: List[MetricElement.Timer]): Map[MetricId, MetricElement.TimerData] =
    timers.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.Timer(MetricKind.Timer.Default, _) =>
          tm.metricId -> tm.timer
      }
    }.toMap

  def meter(meters: List[MetricElement.Meter]): Map[MetricId, MetricElement.MeterData] =
    meters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.Meter(MetricKind.Meter.Default, _) =>
          tm.metricId -> tm.meter
      }
    }.toMap

  def histogram(histograms: List[MetricElement.Histogram]): Map[MetricId, MetricElement.HistogramData] =
    histograms.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.Histogram(MetricKind.Histogram.Default, _) =>
          tm.metricId -> tm.histogram
      }
    }.toMap
}
