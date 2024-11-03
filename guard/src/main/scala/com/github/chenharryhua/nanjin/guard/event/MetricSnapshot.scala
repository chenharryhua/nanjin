package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID}
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.jawn.{decode, parse}
import org.typelevel.cats.time.instances.duration
import squants.time.{Frequency, Hertz}

import java.time.Duration
import scala.jdk.CollectionConverters.*

sealed trait Snapshot extends Product with Serializable { def metricId: MetricID }

object Snapshot {
  @JsonCodec
  final case class Counter(metricId: MetricID, count: Long) extends Snapshot
  @JsonCodec
  final case class Gauge(metricId: MetricID, value: Json) extends Snapshot

  @JsonCodec
  final case class MeterData(
    unit: MeasurementUnit,
    sum: Long,
    mean_rate: Frequency,
    m1_rate: Frequency,
    m5_rate: Frequency,
    m15_rate: Frequency
  )

  @JsonCodec
  final case class Meter(metricId: MetricID, meter: MeterData) extends Snapshot

  @JsonCodec
  final case class TimerData(
    calls: Long,
    mean_rate: Frequency,
    m1_rate: Frequency,
    m5_rate: Frequency,
    m15_rate: Frequency,
    min: Duration,
    max: Duration,
    mean: Duration,
    stddev: Duration,
    p50: Duration,
    p75: Duration,
    p95: Duration,
    p98: Duration,
    p99: Duration,
    p999: Duration
  )
  @JsonCodec
  final case class Timer(metricId: MetricID, timer: TimerData) extends Snapshot

  @JsonCodec
  final case class HistogramData(
    unit: MeasurementUnit,
    updates: Long,
    min: Long,
    max: Long,
    mean: Double,
    stddev: Double,
    p50: Double,
    p75: Double,
    p95: Double,
    p98: Double,
    p99: Double,
    p999: Double
  )

  @JsonCodec
  final case class Histogram(metricId: MetricID, histogram: HistogramData) extends Snapshot
}

@JsonCodec
final case class MetricSnapshot(
  counters: List[Snapshot.Counter],
  meters: List[Snapshot.Meter],
  timers: List[Snapshot.Timer],
  histograms: List[Snapshot.Histogram],
  gauges: List[Snapshot.Gauge]) {
  def isEmpty: Boolean =
    counters.isEmpty && meters.isEmpty && timers.isEmpty && histograms.isEmpty && gauges.isEmpty

  def nonEmpty: Boolean = !isEmpty

  private def metricIDs: List[MetricID] =
    counters.map(_.metricId) :::
      timers.map(_.metricId) :::
      gauges.map(_.metricId) :::
      meters.map(_.metricId) :::
      histograms.map(_.metricId)

  def hasDuplication: Boolean = {
    val stable = metricIDs.map(id => (id.metricName, id.metricTag, id.category))
    stable.distinct.size != stable.size
  }
}

object MetricSnapshot extends duration {

  def counters(metricRegistry: MetricRegistry): List[Snapshot.Counter] =
    metricRegistry.getCounters().asScala.toList.mapFilter { case (name, counter) =>
      decode[MetricID](name).toOption.map(id => Snapshot.Counter(id, counter.getCount))
    }

  def meters(metricRegistry: MetricRegistry): List[Snapshot.Meter] =
    metricRegistry.getMeters().asScala.toList.mapFilter { case (name, meter) =>
      decode[MetricID](name).toOption.collect { id =>
        id.category match {
          case Category.Meter(_, unit) =>
            Snapshot.Meter(
              metricId = id,
              Snapshot.MeterData(
                unit = unit,
                sum = meter.getCount,
                mean_rate = Hertz(meter.getMeanRate),
                m1_rate = Hertz(meter.getOneMinuteRate),
                m5_rate = Hertz(meter.getFiveMinuteRate),
                m15_rate = Hertz(meter.getFifteenMinuteRate)
              )
            )
        }
      }
    }

  def timers(metricRegistry: MetricRegistry): List[Snapshot.Timer] =
    metricRegistry.getTimers().asScala.toList.mapFilter { case (name, timer) =>
      decode[MetricID](name).toOption.map { id =>
        val ss = timer.getSnapshot
        Snapshot.Timer(
          metricId = id,
          Snapshot.TimerData(
            calls = timer.getCount,
            // meter
            mean_rate = Hertz(timer.getMeanRate),
            m1_rate = Hertz(timer.getOneMinuteRate),
            m5_rate = Hertz(timer.getFiveMinuteRate),
            m15_rate = Hertz(timer.getFifteenMinuteRate),
            // histogram
            min = Duration.ofNanos(ss.getMin),
            max = Duration.ofNanos(ss.getMax),
            mean = Duration.ofNanos(ss.getMean.toLong),
            stddev = Duration.ofNanos(ss.getStdDev.toLong),
            p50 = Duration.ofNanos(ss.getMedian.toLong),
            p75 = Duration.ofNanos(ss.get75thPercentile().toLong),
            p95 = Duration.ofNanos(ss.get95thPercentile().toLong),
            p98 = Duration.ofNanos(ss.get98thPercentile().toLong),
            p99 = Duration.ofNanos(ss.get99thPercentile().toLong),
            p999 = Duration.ofNanos(ss.get999thPercentile().toLong)
          )
        )
      }
    }

  def histograms(metricRegistry: MetricRegistry): List[Snapshot.Histogram] =
    metricRegistry.getHistograms().asScala.toList.mapFilter { case (name, histo) =>
      decode[MetricID](name).toOption.collect { id =>
        id.category match {
          case Category.Histogram(_, unit) =>
            val ss = histo.getSnapshot
            Snapshot.Histogram(
              metricId = id,
              Snapshot.HistogramData(
                unit = unit,
                updates = histo.getCount,
                min = ss.getMin,
                max = ss.getMax,
                mean = ss.getMean,
                stddev = ss.getStdDev,
                p50 = ss.getMedian,
                p75 = ss.get75thPercentile(),
                p95 = ss.get95thPercentile(),
                p98 = ss.get98thPercentile(),
                p99 = ss.get99thPercentile(),
                p999 = ss.get999thPercentile()
              )
            )
        }
      }
    }

  def gauges(metricRegistry: MetricRegistry): List[Snapshot.Gauge] =
    metricRegistry.getGauges().asScala.toList.mapFilter { case (name, gauge) =>
      (decode[MetricID](name), parse(gauge.getValue.toString))
        .mapN((id, json) => Snapshot.Gauge(id, json))
        .toOption
    }

  def apply(metricRegistry: MetricRegistry): MetricSnapshot =
    MetricSnapshot(
      counters = counters(metricRegistry),
      meters = meters(metricRegistry),
      timers = timers(metricRegistry),
      histograms = histograms(metricRegistry),
      gauges = gauges(metricRegistry)
    )
}
