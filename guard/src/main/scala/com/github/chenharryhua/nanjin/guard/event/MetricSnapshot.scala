package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.kernel.Monoid
import cats.syntax.all.*
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.{Digested, ServiceParams}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import io.circe.parser.decode
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

sealed abstract class MetricCategory(override val entryName: String) extends EnumEntry

object MetricCategory
    extends CatsEnum[MetricCategory] with Enum[MetricCategory] with CirceEnum[MetricCategory] {
  override val values: IndexedSeq[MetricCategory] = findValues

  case object ActionTimer extends MetricCategory("action.timer")
  case object ActionSuccCounter extends MetricCategory("action.succ")
  case object ActionFailCounter extends MetricCategory("action.fail")

  case object Meter extends MetricCategory("meter")
  case object MeterCounter extends MetricCategory("meter.recently")

  case object Histogram extends MetricCategory("histogram")
  case object HistogramCounter extends MetricCategory("histogram.recently")

  case object Counter extends MetricCategory("count")

  case object Gauge extends MetricCategory("gauge")

  case object PassThroughCounter extends MetricCategory("passThrough")

  case object AlertErrorCounter extends MetricCategory("alert.error")
  case object AlertWarnCounter extends MetricCategory("alert.warn")
  case object AlertInfoCounter extends MetricCategory("alert.info")
}

@JsonCodec
final case class MetricName(digested: Digested, category: MetricCategory) {
  override val toString: String = s"${digested.show}.${category.entryName}"
}

object MetricName {
  implicit val showMetricName: Show[MetricName] = Show.fromToString
}

sealed trait Snapshot { def metricName: MetricName }
object Snapshot {

  @JsonCodec
  final case class Counter(metricName: MetricName, count: Long) extends Snapshot

  @JsonCodec final case class Meter(
    metricName: MetricName,
    count: Long,
    mean_rate: Double,
    m1_rate: Double,
    m5_rate: Double,
    m15_rate: Double
  ) extends Snapshot

  @JsonCodec
  final case class Timer(
    metricName: MetricName,
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
  ) extends Snapshot

  @JsonCodec
  final case class Histogram(
    metricName: MetricName,
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
      extends Snapshot

  @JsonCodec
  final case class Gauge(metricName: MetricName, value: String) extends Snapshot
}

@JsonCodec
final case class MetricSnapshot(
  counters: List[Snapshot.Counter],
  meters: List[Snapshot.Meter],
  timers: List[Snapshot.Timer],
  histograms: List[Snapshot.Histogram],
  gauges: List[Snapshot.Gauge])

object MetricSnapshot extends duration {

  implicit val showMetricSnapshot: Show[MetricSnapshot] = cats.derived.semiauto.show[MetricSnapshot]

  implicit val monoidMetricFilter: Monoid[MetricFilter] = new Monoid[MetricFilter] {
    override val empty: MetricFilter = MetricFilter.ALL

    override def combine(x: MetricFilter, y: MetricFilter): MetricFilter =
      (name: String, metric: Metric) => x.matches(name, metric) && y.matches(name, metric)
  }

  private val positiveFilter: MetricFilter =
    (_: String, metric: Metric) =>
      metric match {
        case c: Counting => c.getCount > 0
        case _           => true
      }

  private def counters(metricRegistry: MetricRegistry, metricFilter: MetricFilter): List[Snapshot.Counter] =
    metricRegistry.getCounters(metricFilter).asScala.toList.mapFilter { case (name, counter) =>
      decode[MetricName](name).toOption.map(Snapshot.Counter(_, counter.getCount))
    }

  private def meters(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit): List[Snapshot.Meter] = {
    val rateFactor = rateTimeUnit.toSeconds(1)
    metricRegistry.getMeters(metricFilter).asScala.toList.mapFilter { case (name, meter) =>
      decode[MetricName](name).toOption.map(mn =>
        Snapshot.Meter(
          metricName = mn,
          count = meter.getCount,
          mean_rate = meter.getMeanRate * rateFactor,
          m1_rate = meter.getOneMinuteRate * rateFactor,
          m5_rate = meter.getFiveMinuteRate * rateFactor,
          m15_rate = meter.getFifteenMinuteRate * rateFactor
        ))
    }
  }

  private def timers(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit): List[Snapshot.Timer] = {
    val rateFactor = rateTimeUnit.toSeconds(1)
    metricRegistry.getTimers(metricFilter).asScala.toList.mapFilter { case (name, timer) =>
      decode[MetricName](name).toOption.map { mn =>
        val ss = timer.getSnapshot
        Snapshot.Timer(
          metricName = mn,
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
  }

  private def histograms(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter): List[Snapshot.Histogram] =
    metricRegistry.getHistograms(metricFilter).asScala.toList.mapFilter { case (name, histo) =>
      decode[MetricName](name).toOption.map { mn =>
        val ss = histo.getSnapshot
        Snapshot.Histogram(
          metricName = mn,
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
    }

  private def gauges(metricRegistry: MetricRegistry, metricFilter: MetricFilter): List[Snapshot.Gauge] =
    metricRegistry.getGauges(metricFilter).asScala.toList.mapFilter { case (name, gauge) =>
      decode[MetricName](name).toOption.map(Snapshot.Gauge(_, gauge.getValue.toString))
    }

  def apply(
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams,
    filter: MetricFilter): MetricSnapshot = {
    val newFilter = filter |+| positiveFilter
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
