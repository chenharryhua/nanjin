package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.catsSyntaxSemigroup
import cats.kernel.Monoid
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import io.circe.generic.JsonCodec

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

@JsonCodec
final case class CounterSnapshot(name: String, count: Long)

@JsonCodec final case class MeterSnapshot(
  name: String,
  count: Long,
  mean_rate: Double,
  m1_rate: Double,
  m5_rate: Double,
  m15_rate: Double,
  units: String
)

@JsonCodec
final case class TimerSnapshot(
  name: String,
  count: Long,
  mean_rate: Double,
  m1_rate: Double,
  m5_rate: Double,
  m15_rate: Double,
  min: Double,
  max: Double,
  mean: Double,
  stddev: Double,
  median: Double,
  p75: Double,
  p95: Double,
  p98: Double,
  p99: Double,
  p999: Double,
  duration_units: String,
  rate_units: String
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
  gauges: List[GaugeSnapshot],
  show: String) {
  override def toString: String = show
}

object MetricSnapshot {

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

  implicit val showSnapshot: Show[MetricSnapshot] = _.show

  private def toText(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit,
    zoneId: ZoneId): String = {
    val bao = new ByteArrayOutputStream
    val ps  = new PrintStream(bao)
    ConsoleReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(rateTimeUnit)
      .convertDurationsTo(durationTimeUnit)
      .formattedFor(TimeZone.getTimeZone(zoneId))
      .filter(metricFilter)
      .outputTo(ps)
      .build()
      .report()
    ps.flush()
    ps.close()
    bao.toString(StandardCharsets.UTF_8.name())
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
        m15_rate = meter.getFifteenMinuteRate * rateFactor,
        units = rateTimeUnit.name()
      )
    }
  }

  private def timers(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit): List[TimerSnapshot] = {
    val rateFactor     = rateTimeUnit.toSeconds(1)
    val durationFactor = durationTimeUnit.toNanos(1)
    metricRegistry.getTimers(metricFilter).asScala.toList.map { case (name, timer) =>
      val ss = timer.getSnapshot
      TimerSnapshot(
        name = name,
        count = timer.getCount,
        max = ss.getMax.toDouble / durationFactor,
        mean = ss.getMean / durationFactor,
        min = ss.getMin.toDouble / durationFactor,
        median = ss.getMedian / durationFactor,
        p75 = ss.get75thPercentile() / durationFactor,
        p95 = ss.get95thPercentile() / durationFactor,
        p98 = ss.get98thPercentile() / durationFactor,
        p99 = ss.get99thPercentile() / durationFactor,
        p999 = ss.get999thPercentile() / durationFactor,
        stddev = ss.getStdDev / durationFactor,
        m1_rate = timer.getOneMinuteRate * rateFactor,
        m5_rate = timer.getFiveMinuteRate * rateFactor,
        m15_rate = timer.getFifteenMinuteRate * rateFactor,
        mean_rate = timer.getMeanRate * rateFactor,
        duration_units = durationTimeUnit.name(),
        rate_units = rateTimeUnit.name()
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
    val newFilter = filter |+| positiveFilter
    MetricSnapshot(
      counters(metricRegistry, newFilter),
      meters(metricRegistry, newFilter, serviceParams.metricParams.rateTimeUnit),
      timers(
        metricRegistry,
        newFilter,
        serviceParams.metricParams.rateTimeUnit,
        serviceParams.metricParams.durationTimeUnit),
      histograms(metricRegistry, newFilter),
      gauges(metricRegistry, newFilter),
      toText(
        metricRegistry,
        newFilter,
        serviceParams.metricParams.rateTimeUnit,
        serviceParams.metricParams.durationTimeUnit,
        serviceParams.taskParams.zoneId)
    )
  }
}
