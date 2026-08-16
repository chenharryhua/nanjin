package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import com.github.chenharryhua.nanjin.guard.metrics.{Category, CategoryKind, MetricID}
import io.circe.Decoder

object retrieve {
  def healthCheck(gauges: List[MetricElement.Gauge]): Map[MetricID, Boolean] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(CategoryKind.GaugeKind.HealthCheck) =>
          gg.gauge.value.asBoolean.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def gauge[A: Decoder](gauges: List[MetricElement.Gauge]): Map[MetricID, A] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(CategoryKind.GaugeKind.Gauge) =>
          gg.gauge.value.as[A].toOption.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def counter(counters: List[MetricElement.Counter]): Map[MetricID, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case Category.Counter(CategoryKind.CounterKind.Counter) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def riskCounter(counters: List[MetricElement.Counter]): Map[MetricID, MetricElement.CounterData] =
    counters.collect { tm =>
      tm.metricId.category match {
        case Category.Counter(CategoryKind.CounterKind.Risk) =>
          tm.metricId -> tm.counter
      }
    }.toMap

  def timer(timers: List[MetricElement.Timer]): Map[MetricID, MetricElement.TimerData] =
    timers.collect { tm =>
      tm.metricId.category match {
        case Category.Timer(CategoryKind.TimerKind.Timer) =>
          tm.metricId -> tm.timer
      }
    }.toMap

  def meter(meters: List[MetricElement.Meter]): Map[MetricID, MetricElement.MeterData] =
    meters.collect { tm =>
      tm.metricId.category match {
        case Category.Meter(CategoryKind.MeterKind.Meter, _) =>
          tm.metricId -> tm.meter
      }
    }.toMap

  def histogram(histograms: List[MetricElement.Histogram]): Map[MetricID, MetricElement.HistogramData] =
    histograms.collect { tm =>
      tm.metricId.category match {
        case Category.Histogram(CategoryKind.HistogramKind.Histogram, _) =>
          tm.metricId -> tm.histogram
      }
    }.toMap
}
