package com.github.chenharryhua.nanjin.guard.observers

import com.github.chenharryhua.nanjin.guard.event.Snapshot

import java.time.Duration

sealed private trait HistogramField {
  def pick(timer: Snapshot.Timer): (Duration, String)
  def pick(histo: Snapshot.Histogram): (Double, String)
}

private object HistogramField {
  case object Min extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.min, METRICS_MIN)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.min.toDouble, METRICS_MIN)
  }
  case object Max extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.max, METRICS_MAX)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.max.toDouble, METRICS_MAX)
  }
  case object Mean extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.mean, METRICS_MEAN)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.mean, METRICS_MEAN)
  }
  case object StdDev extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.stddev, METRICS_STD_DEV)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.stddev, METRICS_STD_DEV)
  }
  case object P50 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p50, METRICS_P50)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p50, METRICS_P50)
  }
  case object P75 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p75, METRICS_P75)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p75, METRICS_P75)
  }
  case object P95 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p95, METRICS_P95)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p95, METRICS_P95)
  }
  case object P98 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p98, METRICS_P98)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p98, METRICS_P98)
  }
  case object P99 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p99, METRICS_P99)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p99, METRICS_P99)
  }
  case object P999 extends HistogramField {
    override def pick(timer: Snapshot.Timer): (Duration, String)   = (timer.data.p999, METRICS_P999)
    override def pick(histo: Snapshot.Histogram): (Double, String) = (histo.data.p999, METRICS_P999)
  }
}
