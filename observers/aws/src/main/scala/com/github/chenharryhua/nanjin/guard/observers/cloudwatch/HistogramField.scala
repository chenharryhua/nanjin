package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import com.github.chenharryhua.nanjin.guard.event.Snapshot

import java.time.Duration

sealed private trait HistogramField {
  def pick(timer: Snapshot.Timer): (Duration, String)
  def pick(histo: Snapshot.Histogram): (Double, String)
}

private object HistogramField {
  import com.github.chenharryhua.nanjin.guard.translator.metricConstants.*
  case object Min extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.min, METRICS_MIN)
    override def pick(histo: Snapshot.Histogram): (Double, String) =
      (histo.histogram.min.toDouble, METRICS_MIN)
  }
  case object Max extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.max, METRICS_MAX)
    override def pick(histo: Snapshot.Histogram): (Double, String) =
      (histo.histogram.max.toDouble, METRICS_MAX)
  }
  case object Mean extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.mean, METRICS_MEAN)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.mean, METRICS_MEAN)
  }
  case object StdDev extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.stddev, METRICS_STD_DEV)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.stddev, METRICS_STD_DEV)
  }
  case object P50 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p50, METRICS_P50)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p50, METRICS_P50)
  }
  case object P75 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p75, METRICS_P75)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p75, METRICS_P75)
  }
  case object P95 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p95, METRICS_P95)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p95, METRICS_P95)
  }
  case object P98 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p98, METRICS_P98)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p98, METRICS_P98)
  }
  case object P99 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p99, METRICS_P99)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p99, METRICS_P99)
  }
  case object P999 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.timer.p999, METRICS_P999)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.histogram.p999, METRICS_P999)
  }
}
