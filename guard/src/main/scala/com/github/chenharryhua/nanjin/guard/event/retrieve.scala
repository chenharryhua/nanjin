package com.github.chenharryhua.nanjin.guard.event

import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.*
import io.circe.Decoder

object retrieveHealthChecks {
  def apply(gauges: List[Snapshot.Gauge]): Map[MetricID, Boolean] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(GaugeKind.HealthCheck, _) =>
          gg.value.asBoolean.map(gg.metricId -> _)
      }
    }.flatten.toMap

  def apply(metricRegistry: MetricRegistry): Map[MetricID, Boolean] =
    apply(MetricSnapshot.gauges(metricRegistry))
}

object retrieveGauge {
  def apply[A: Decoder](gauges: List[Snapshot.Gauge]): Map[MetricID, A] =
    gauges.collect { gg =>
      gg.metricId.category match {
        case Category.Gauge(GaugeKind.Gauge, _) =>
          gg.value.as[A].toOption.map(gg.metricId -> _)
      }
    }.flatten.toMap
}

object retrieveAction {
  object timer {
    def apply(timers: List[Snapshot.Timer]): Map[MetricID, Snapshot.TimerData] =
      timers.collect { tm =>
        tm.metricId.category match {
          case Category.Timer(TimerKind.Action, _) =>
            tm.metricId -> tm.timer
        }
      }.toMap
  }

  object doneCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.ActionDone, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }

  object retryCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.ActionRetry, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }

  object failCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.ActionFail, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }
}

object retrieveTimer {
  def apply(timers: List[Snapshot.Timer]): Map[MetricID, Snapshot.TimerData] =
    timers.collect { tm =>
      tm.metricId.category match {
        case Category.Timer(TimerKind.Timer, _) =>
          tm.metricId -> tm.timer
      }
    }.toMap
}

object retrieveAlert {
  object errorCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.AlertError, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }

  object warnCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.AlertWarn, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }

  object infoCount {
    def apply(counters: List[Snapshot.Counter]): Map[MetricID, Long] =
      counters.collect { cc =>
        cc.metricId.category match {
          case Category.Counter(CounterKind.AlertInfo, _) =>
            cc.metricId -> cc.count
        }
      }.toMap
  }
}

object retrieveMeter {
  def apply(meters: List[Snapshot.Meter]): Map[MetricID, Snapshot.MeterData] =
    meters.collect { tm =>
      tm.metricId.category match {
        case Category.Meter(MeterKind.Meter, _, _) =>
          tm.metricId -> tm.meter
      }
    }.toMap
}

object retrieveHistogram {
  def apply(histograms: List[Snapshot.Histogram]): Map[MetricID, Snapshot.HistogramData] =
    histograms.collect { tm =>
      tm.metricId.category match {
        case Category.Histogram(HistogramKind.Histogram, _, _) =>
          tm.metricId -> tm.histogram
      }
    }.toMap
}

object retrieveMetricIds {
  def apply(mss: MetricSnapshot): List[MetricID] =
    mss.counters.map(_.metricId) :::
      mss.timers.map(_.metricId) :::
      mss.gauges.map(_.metricId) :::
      mss.meters.map(_.metricId) :::
      mss.histograms.map(_.metricId)
}
