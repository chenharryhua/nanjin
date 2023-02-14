package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.catsSyntaxSemigroup
import cats.kernel.Monoid
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.jdk.CollectionConverters.*

sealed abstract class SnapshotCategory(val name: String) extends EnumEntry with Product with Serializable

object SnapshotCategory
    extends Enum[SnapshotCategory] with CirceEnum[SnapshotCategory] with CatsEnum[SnapshotCategory] {
  override val values: immutable.IndexedSeq[SnapshotCategory] = findValues
  case object Meter extends SnapshotCategory("meter")
  case object Gauge extends SnapshotCategory("gauge")
  case object Counter extends SnapshotCategory("counter")
  case object Timer extends SnapshotCategory("timer")
  case object Histogram extends SnapshotCategory("histogram")
}

@JsonCodec
final case class CounterSnapshot(name: String, count: Long)

@JsonCodec final case class MeterSnapshot(
  name: String,
  count: Long,
  mean_rate: Double,
  m1_rate: Double,
  m5_rate: Double,
  m15_rate: Double
)

@JsonCodec
final case class TimerSnapshot(
  name: String,
  count: Long,
  mean_rate: Double,
  m1_rate: Double,
  m5_rate: Double,
  m15_rate: Double,
  min: Duration,
  max: Duration,
  mean: Duration,
  stddev: Duration,
  median: Duration,
  p75: Duration,
  p95: Duration,
  p98: Duration,
  p99: Duration,
  p999: Duration
)

@JsonCodec
final case class HistogramSnapshot(
  name: String,
  count: Long,
  min: Long,
  max: Long,
  mean: Double,
  stddev: Double,
  median: Double,
  p75: Double,
  p95: Double,
  p98: Double,
  p99: Double,
  p999: Double)

@JsonCodec
final case class GaugeSnapshot(name: String, value: String)

@JsonCodec
final case class MetricSnapshot(
  counters: List[CounterSnapshot],
  meters: List[MeterSnapshot],
  timers: List[TimerSnapshot],
  histograms: List[HistogramSnapshot],
  gauges: List[GaugeSnapshot] )

object MetricSnapshot extends duration {

  implicit val showMetricSnapshot: Show[MetricSnapshot] = cats.derived.semiauto.show[MetricSnapshot]

  implicit val monoidMetricFilter: Monoid[MetricFilter] = new Monoid[MetricFilter] {
    override val empty: MetricFilter = MetricFilter.ALL

    override def combine(x: MetricFilter, y: MetricFilter): MetricFilter =
      (name: String, metric: Metric) => x.matches(name, metric) && y.matches(name, metric)
  }

  val positiveFilter: MetricFilter =
    (_: String, metric: Metric) =>
      metric match {
        case c: Counting => c.getCount > 0
        case _           => true
      }

  private def counters(metricRegistry: MetricRegistry, metricFilter: MetricFilter): List[CounterSnapshot] =
    metricRegistry.getCounters(metricFilter).asScala.toList.map { case (name, counter) =>
      CounterSnapshot(name, counter.getCount)
    }

  private def meters(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit): List[MeterSnapshot] = {
    val rateFactor = rateTimeUnit.toSeconds(1)
    metricRegistry.getMeters(metricFilter).asScala.toList.map { case (name, meter) =>
      MeterSnapshot(
        name = name,
        count = meter.getCount,
        mean_rate = meter.getMeanRate * rateFactor,
        m1_rate = meter.getOneMinuteRate * rateFactor,
        m5_rate = meter.getFiveMinuteRate * rateFactor,
        m15_rate = meter.getFifteenMinuteRate * rateFactor
      )
    }
  }

  private def timers(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit): List[TimerSnapshot] = {
    val rateFactor = rateTimeUnit.toSeconds(1)
    metricRegistry.getTimers(metricFilter).asScala.toList.map { case (name, timer) =>
      val ss = timer.getSnapshot
      TimerSnapshot(
        name = name,
        count = timer.getCount,
        // meter
        mean_rate = timer.getMeanRate * rateFactor,
        m1_rate = timer.getOneMinuteRate * rateFactor,
        m5_rate = timer.getFiveMinuteRate * rateFactor,
        m15_rate = timer.getFifteenMinuteRate * rateFactor,
        // histogram
        min = Duration.ofNanos(ss.getMin),
        max = Duration.ofNanos(ss.getMax),
        mean = Duration.ofNanos(ss.getMean.toLong),
        stddev = Duration.ofNanos(ss.getStdDev.toLong),
        median = Duration.ofNanos(ss.getMedian.toLong),
        p75 = Duration.ofNanos(ss.get75thPercentile().toLong),
        p95 = Duration.ofNanos(ss.get95thPercentile().toLong),
        p98 = Duration.ofNanos(ss.get98thPercentile().toLong),
        p99 = Duration.ofNanos(ss.get99thPercentile().toLong),
        p999 = Duration.ofNanos(ss.get999thPercentile().toLong)
      )
    }
  }

  private def histograms(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter): List[HistogramSnapshot] =
    metricRegistry.getHistograms(metricFilter).asScala.toList.map { case (name, histo) =>
      val ss = histo.getSnapshot
      HistogramSnapshot(
        name = name,
        count = histo.getCount,
        max = ss.getMax,
        mean = ss.getMean,
        min = ss.getMin,
        median = ss.getMedian,
        p75 = ss.get75thPercentile(),
        p95 = ss.get95thPercentile(),
        p98 = ss.get98thPercentile(),
        p99 = ss.get99thPercentile(),
        p999 = ss.get999thPercentile(),
        stddev = ss.getStdDev
      )
    }

  private def gauges(metricRegistry: MetricRegistry, metricFilter: MetricFilter): List[GaugeSnapshot] =
    metricRegistry.getGauges(metricFilter).asScala.toList.map { case (name, gauge) =>
      GaugeSnapshot(name, gauge.getValue.toString)
    }

  def apply(
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams,
    filter: MetricFilter): MetricSnapshot = {
    val newFilter  = filter |+| positiveFilter
    val rate_unit = serviceParams.metricParams.rateTimeUnit
    MetricSnapshot(
      counters = counters(metricRegistry, newFilter),
      meters = meters(metricRegistry, newFilter, rate_unit),
      timers = timers(metricRegistry, newFilter, rate_unit),
      histograms = histograms(metricRegistry, newFilter),
      gauges = gauges(metricRegistry, newFilter)
    )
  }
}
