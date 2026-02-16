package com.github.chenharryhua.nanjin.guard.event

import cats.effect.implicits.clockOps
import cats.effect.kernel.Sync
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{MetricID, Squants}
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.jawn.{decode, parse}
import org.typelevel.cats.time.instances.duration
import squants.time.{Frequency, Hertz}

import java.time.Duration
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait Snapshot extends Product { def metricId: MetricID }

object Snapshot {
  @JsonCodec
  final case class Counter(metricId: MetricID, count: Long) extends Snapshot
  @JsonCodec
  final case class Gauge(metricId: MetricID, value: Json) extends Snapshot

  @JsonCodec
  final case class MeterData(
    squants: Squants,
    aggregate: Long,
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
    squants: Squants,
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

  def metricIDs: List[MetricID] =
    counters.map(_.metricId) :::
      timers.map(_.metricId) :::
      gauges.map(_.metricId) :::
      meters.map(_.metricId) :::
      histograms.map(_.metricId)

  def hasDuplication: Boolean = {
    val stable = metricIDs.map(id => (id.metricLabel, id.metricName.name))
    stable.distinct.size =!= stable.size
  }

  def lookupCount: Map[UUID, Long] = {
    meters.map(m => m.metricId.metricName.uuid -> m.meter.aggregate) :::
      timers.map(t => t.metricId.metricName.uuid -> t.timer.calls) :::
      histograms.map(h => h.metricId.metricName.uuid -> h.histogram.updates)
  }.toMap

  def sorted: MetricSnapshot = MetricSnapshot(
    counters = counters.sortBy(_.metricId.metricName),
    meters = meters.sortBy(_.metricId.metricName),
    timers = timers.sortBy(_.metricId.metricName),
    histograms = histograms.sortBy(_.metricId.metricName),
    gauges = gauges.sortBy(_.metricId.metricName)
  )
}

sealed trait ScrapeMode
object ScrapeMode {
  case object Cheap extends ScrapeMode
  case object Full extends ScrapeMode
}

object MetricSnapshot extends duration {
  val empty: MetricSnapshot = MetricSnapshot(Nil, Nil, Nil, Nil, Nil)

  private def buildFrom(metricRegistry: MetricRegistry, mode: ScrapeMode): MetricSnapshot = {
    // counters
    val counters: List[Snapshot.Counter] =
      metricRegistry.getCounters.asScala.foldLeft(List.empty[Snapshot.Counter]) {
        case (lst, (name, counter)) =>
          decode[MetricID](name) match {
            case Left(_)    => lst
            case Right(mid) => Snapshot.Counter(metricId = mid, count = counter.getCount) :: lst
          }
      }

    // meters
    val meters: List[Snapshot.Meter] =
      metricRegistry.getMeters().asScala.foldLeft(List.empty[Snapshot.Meter]) { case (lst, (name, meter)) =>
        decode[MetricID](name) match {
          case Left(_)    => lst
          case Right(mid) =>
            mid.isMeter.fold(lst) { cat =>
              Snapshot.Meter(
                metricId = mid,
                Snapshot.MeterData(
                  squants = cat.squants,
                  aggregate = meter.getCount,
                  mean_rate = Hertz(meter.getMeanRate),
                  m1_rate = Hertz(meter.getOneMinuteRate),
                  m5_rate = Hertz(meter.getFiveMinuteRate),
                  m15_rate = Hertz(meter.getFifteenMinuteRate)
                )
              ) :: lst
            }
        }
      }

    // histograms
    val histograms = metricRegistry.getHistograms().asScala.foldLeft(List.empty[Snapshot.Histogram]) {
      case (lst, (name, histogram)) =>
        decode[MetricID](name) match {
          case Left(_)    => lst
          case Right(mid) =>
            mid.isHisto.fold(lst) { cat =>
              val ss = histogram.getSnapshot
              Snapshot.Histogram(
                metricId = mid,
                Snapshot.HistogramData(
                  squants = cat.squants,
                  updates = histogram.getCount,
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
              ) :: lst
            }
        }
    }

    // timers
    val timers: List[Snapshot.Timer] =
      metricRegistry.getTimers().asScala.foldLeft(List.empty[Snapshot.Timer]) { case (lst, (name, timer)) =>
        decode[MetricID](name) match {
          case Left(_)    => lst
          case Right(mid) =>
            val ss = timer.getSnapshot
            Snapshot.Timer(
              metricId = mid,
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
            ) :: lst
        }
      }

    // gauges
    val gauges: List[Snapshot.Gauge] =
      mode match {
        case ScrapeMode.Cheap => Nil
        case ScrapeMode.Full  =>
          metricRegistry.getGauges().asScala.foldLeft(List.empty[Snapshot.Gauge]) {
            case (lst, (name, gauge)) =>
              decode[MetricID](name) match {
                case Left(_)    => lst
                case Right(mid) =>
                  parse(gauge.getValue.toString) match {
                    case Right(json) => Snapshot.Gauge(mid, json) :: lst
                    case Left(_)     => lst
                  }
              }
          }
      }

    MetricSnapshot(
      counters = counters,
      meters = meters,
      timers = timers,
      histograms = histograms,
      gauges = gauges)
  }

  def timed[F[_]](metricRegistry: metrics.MetricRegistry, mode: ScrapeMode)(implicit
    F: Sync[F]): F[(Duration, MetricSnapshot)] =
    F.blocking(buildFrom(metricRegistry, mode)).timed.map { case (fd, ss) => (fd.toJava, ss) }
}
