package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.kernel.Monoid
import cats.syntax.all.*
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.Digested
import io.circe.generic.JsonCodec
import io.circe.parser.decode
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import scala.jdk.CollectionConverters.*

@JsonCodec
sealed abstract private[guard] class MetricCategory(val value: String)

private[guard] object MetricCategory {

  case object ActionTimer extends MetricCategory("timer")
  case object ActionCompleteCounter extends MetricCategory("action.done")
  case object ActionFailCounter extends MetricCategory("action.fail")
  case object ActionRetryCounter extends MetricCategory("action.retry")

  case object Meter extends MetricCategory("meter")
  case object MeterCounter extends MetricCategory("meter.recently")

  final case class Histogram(unitOfMeasure: String) extends MetricCategory("histogram")
  case object HistogramCounter extends MetricCategory("histogram.updates")

  case object Counter extends MetricCategory("count")

  case object Gauge extends MetricCategory("gauge")

  case object PassThroughCounter extends MetricCategory("passThrough")

  case object AlertErrorCounter extends MetricCategory("alert.error")
  case object AlertWarnCounter extends MetricCategory("alert.warn")
  case object AlertInfoCounter extends MetricCategory("alert.info")
}

@JsonCodec
final private[guard] case class MetricID(digested: Digested, category: MetricCategory)

trait Snapshot { def digested: Digested }
object Snapshot {

  @JsonCodec
  final case class Counter(digested: Digested, category: String, count: Long) extends Snapshot

  @JsonCodec final case class Meter(
    digested: Digested,
    count: Long,
    mean_rate: Double,
    m1_rate: Double,
    m5_rate: Double,
    m15_rate: Double
  ) extends Snapshot

  @JsonCodec
  final case class Timer(
    digested: Digested,
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
    digested: Digested,
    unitOfMeasure: String,
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
  final case class Gauge(digested: Digested, value: String) extends Snapshot
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

  private def counters(metricRegistry: MetricRegistry): List[Snapshot.Counter] =
    metricRegistry.getCounters().asScala.toList.mapFilter { case (name, counter) =>
      decode[MetricID](name).toOption.map(mn =>
        Snapshot.Counter(mn.digested, mn.category.value, counter.getCount))
    }

  private def meters(metricRegistry: MetricRegistry): List[Snapshot.Meter] =
    metricRegistry.getMeters().asScala.toList.mapFilter { case (name, meter) =>
      decode[MetricID](name).toOption.map(mn =>
        Snapshot.Meter(
          digested = mn.digested,
          count = meter.getCount,
          mean_rate = meter.getMeanRate,
          m1_rate = meter.getOneMinuteRate,
          m5_rate = meter.getFiveMinuteRate,
          m15_rate = meter.getFifteenMinuteRate
        ))
    }

  private def timers(metricRegistry: MetricRegistry): List[Snapshot.Timer] =
    metricRegistry.getTimers().asScala.toList.mapFilter { case (name, timer) =>
      decode[MetricID](name).toOption.map { mn =>
        val ss = timer.getSnapshot
        Snapshot.Timer(
          digested = mn.digested,
          count = timer.getCount,
          // meter
          mean_rate = timer.getMeanRate,
          m1_rate = timer.getOneMinuteRate,
          m5_rate = timer.getFiveMinuteRate,
          m15_rate = timer.getFifteenMinuteRate,
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

  private def histograms(metricRegistry: MetricRegistry): List[Snapshot.Histogram] =
    metricRegistry.getHistograms().asScala.toList.mapFilter { case (name, histo) =>
      decode[MetricID](name).toOption.flatMap { mn =>
        mn.category match {
          case MetricCategory.Histogram(unitOfMeasure) =>
            val ss = histo.getSnapshot
            Some(
              Snapshot.Histogram(
                digested = mn.digested,
                unitOfMeasure = unitOfMeasure,
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
              ))
          case _ => None
        }
      }
    }

  private def gauges(metricRegistry: MetricRegistry): List[Snapshot.Gauge] =
    metricRegistry.getGauges().asScala.toList.mapFilter { case (name, gauge) =>
      decode[MetricID](name).toOption.map(mn => Snapshot.Gauge(mn.digested, gauge.getValue.toString))
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
