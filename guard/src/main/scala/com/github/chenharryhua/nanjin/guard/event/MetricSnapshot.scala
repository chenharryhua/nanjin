package com.github.chenharryhua.nanjin.guard.event

import cats.effect.implicits.clockOps
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.codahale.metrics
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID}
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.jawn.{decode, parse}
import monocle.Lens
import monocle.macros.GenLens
import org.typelevel.cats.time.instances.duration
import squants.time.{Frequency, Hertz}

import java.time.Duration
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait Snapshot extends Product with Serializable { def metricId: MetricID }

object Snapshot {
  @JsonCodec
  final case class Counter(metricId: MetricID, count: Long) extends Snapshot
  @JsonCodec
  final case class Gauge(metricId: MetricID, value: Json) extends Snapshot

  @JsonCodec
  final case class MeterData(
    unitSymbol: String,
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
    unitSymbol: String,
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
    stable.distinct.size != stable.size
  }

  def lookupCount: Map[UUID, Long] = {
    meters.map(m => m.metricId.metricName.uuid -> m.meter.aggregate) :::
      timers.map(t => t.metricId.metricName.uuid -> t.timer.calls) :::
      histograms.map(h => h.metricId.metricName.uuid -> h.histogram.updates)
  }.toMap
}

object MetricSnapshot extends duration {
  val empty: MetricSnapshot = MetricSnapshot(Nil, Nil, Nil, Nil, Nil)

  private val counterLens: Lens[MetricSnapshot, List[Snapshot.Counter]] =
    GenLens[MetricSnapshot](_.counters)
  private val gaugeLens: Lens[MetricSnapshot, List[Snapshot.Gauge]] =
    GenLens[MetricSnapshot](_.gauges)
  private val meterLens: Lens[MetricSnapshot, List[Snapshot.Meter]] =
    GenLens[MetricSnapshot](_.meters)
  private val histogramLens: Lens[MetricSnapshot, List[Snapshot.Histogram]] =
    GenLens[MetricSnapshot](_.histograms)
  private val timerLens: Lens[MetricSnapshot, List[Snapshot.Timer]] =
    GenLens[MetricSnapshot](_.timers)

  private def buildFrom(metricRegistry: metrics.MetricRegistry): MetricSnapshot =
    metricRegistry.getMetrics.asScala.toList.foldLeft(empty) { case (snapshot, (name, metric)) =>
      decode[MetricID](name) match {
        case Left(_)    => snapshot
        case Right(mid) =>
          mid.category match {
            // gauge
            case Category.Gauge(_) =>
              metric match {
                case gauge: metrics.Gauge[?] =>
                  parse(gauge.getValue.toString) match {
                    case Right(value) =>
                      val g: Snapshot.Gauge = Snapshot.Gauge(mid, value)
                      gaugeLens.modify(g :: _)(snapshot)
                    case Left(_) => snapshot
                  }
                case _ => snapshot
              }

            // counter
            case Category.Counter(_) =>
              metric match {
                case counter: metrics.Counter =>
                  val c: Snapshot.Counter = Snapshot.Counter(mid, counter.getCount)
                  counterLens.modify(c :: _)(snapshot)
                case _ => snapshot
              }

            // meter
            case Category.Meter(_, unitSymbol) =>
              metric match {
                case meter: metrics.Meter =>
                  val m: Snapshot.Meter = Snapshot.Meter(
                    metricId = mid,
                    Snapshot.MeterData(
                      unitSymbol = unitSymbol,
                      aggregate = meter.getCount,
                      mean_rate = Hertz(meter.getMeanRate),
                      m1_rate = Hertz(meter.getOneMinuteRate),
                      m5_rate = Hertz(meter.getFiveMinuteRate),
                      m15_rate = Hertz(meter.getFifteenMinuteRate)
                    )
                  )
                  meterLens.modify(m :: _)(snapshot)
                case _ => snapshot
              }

            // histogram
            case Category.Histogram(_, unitSymbol) =>
              metric match {
                case histogram: metrics.Histogram =>
                  val ss = histogram.getSnapshot
                  val h: Snapshot.Histogram = Snapshot.Histogram(
                    metricId = mid,
                    Snapshot.HistogramData(
                      unitSymbol = unitSymbol,
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
                  )
                  histogramLens.modify(h :: _)(snapshot)
                case _ => snapshot
              }

            // timer
            case Category.Timer(_) =>
              metric match {
                case timer: metrics.Timer =>
                  val ss = timer.getSnapshot
                  val t: Snapshot.Timer = Snapshot.Timer(
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
                  )
                  timerLens.modify(t :: _)(snapshot)
                case _ => snapshot
              }
          }
      }
    }

  def timed[F[_]](metricRegistry: metrics.MetricRegistry)(implicit
    F: Sync[F]): F[(Duration, MetricSnapshot)] =
    F.blocking(buildFrom(metricRegistry)).timed.map { case (fd, ss) => (fd.toJava, ss) }
}
