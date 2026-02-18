package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.*
import io.circe.Decoder

object retrieveHealthChecks {
  def apply(gauges: List[MetricElement.Gauge]): Map[MetricID, Boolean] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(GaugeKind.HealthCheck) =>
          gg.value.asBoolean.map(gg.metricId -> _)
      }
    }.flatten.toMap
}

object retrieveGauge {
  def apply[A: Decoder](gauges: List[MetricElement.Gauge]): Map[MetricID, A] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(GaugeKind.Gauge) =>
          gg.value.as[A].toOption.map(gg.metricId -> _)
      }
    }.flatten.toMap
}

object retrieveCounter {
  def apply(counters: List[MetricElement.Counter]): Map[MetricID, Long] =
    counters.collect { tm =>
      tm.metricId.category match {
        case Category.Counter(CounterKind.Counter) =>
          tm.metricId -> tm.count
      }
    }.toMap
}

object retrieveRiskCounter {
  def apply(counters: List[MetricElement.Counter]): Map[MetricID, Long] =
    counters.collect { tm =>
      tm.metricId.category match {
        case Category.Counter(CounterKind.Risk) =>
          tm.metricId -> tm.count
      }
    }.toMap
}

object retrieveTimer {
  def apply(timers: List[MetricElement.Timer]): Map[MetricID, MetricElement.TimerData] =
    timers.collect { tm =>
      tm.metricId.category match {
        case Category.Timer(TimerKind.Timer) =>
          tm.metricId -> tm.timer
      }
    }.toMap
}

object retrieveMeter {
  def apply(meters: List[MetricElement.Meter]): Map[MetricID, MetricElement.MeterData] =
    meters.collect { tm =>
      tm.metricId.category match {
        case Category.Meter(MeterKind.Meter, _) =>
          tm.metricId -> tm.meter
      }
    }.toMap
}

object retrieveHistogram {
  def apply(histograms: List[MetricElement.Histogram]): Map[MetricID, MetricElement.HistogramData] =
    histograms.collect { tm =>
      tm.metricId.category match {
        case Category.Histogram(HistogramKind.Histogram, _) =>
          tm.metricId -> tm.histogram
      }
    }.toMap
}
