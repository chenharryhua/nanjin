package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricCategoryKind, MetricID}
import io.circe.{Decoder, Json}

object retrieve {
  def healthCheck(gauges: List[MetricElement.Gauge]): Map[MetricID, Boolean] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.GaugeC(MetricCategoryKind.GaugeKind.HealthCheck) =>
          gg.gauge.value.asBoolean.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def gauge[A: Decoder](gauges: List[MetricElement.Gauge]): Map[MetricID, A] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.GaugeC(MetricCategoryKind.GaugeKind.Default) =>
          gg.gauge.value.as[A].toOption.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def percentile(gauges: List[MetricElement.Gauge]): Map[MetricID, Json] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case MetricCategory.GaugeC(MetricCategoryKind.GaugeKind.Percentile) =>
          gg.metricId -> gg.gauge.value
      }
    }.toMap

  def counter(counters: List[MetricElement.Counter]): Map[MetricID, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.CounterC(MetricCategoryKind.CounterKind.Default) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def riskCounter(counters: List[MetricElement.Counter]): Map[MetricID, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.CounterC(MetricCategoryKind.CounterKind.Risk) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def timer(timers: List[MetricElement.Timer]): Map[MetricID, MetricElement.TimerData] =
    timers.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.TimerC(MetricCategoryKind.TimerKind.Default) =>
          tm.metricId -> tm.timer
      }
    }.toMap

  def meter(meters: List[MetricElement.Meter]): Map[MetricID, MetricElement.MeterData] =
    meters.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.MeterC(MetricCategoryKind.MeterKind.Default, _) =>
          tm.metricId -> tm.meter
      }
    }.toMap

  def histogram(histograms: List[MetricElement.Histogram]): Map[MetricID, MetricElement.HistogramData] =
    histograms.collect { tm =>
      tm.metricId.category match {
        case MetricCategory.HistogramC(MetricCategoryKind.HistogramKind.Default, _) =>
          tm.metricId -> tm.histogram
      }
    }.toMap
}
