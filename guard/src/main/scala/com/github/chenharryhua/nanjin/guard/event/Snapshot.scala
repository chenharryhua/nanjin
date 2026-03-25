package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.eq.catsSyntaxEq
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Codec, Decoder, Encoder, Json}
import squants.time.Frequency

import java.time.Duration

sealed trait MetricElement extends Product { def metricId: MetricID }

object MetricElement {
  opaque type CounterData = Long
  object CounterData:
    def apply(c: Long): CounterData = c
    extension (cd: CounterData) def value: Long = cd

    given Encoder[CounterData] = OpaqueLift.lift[CounterData, Long, Encoder]
    given Decoder[CounterData] = OpaqueLift.lift[CounterData, Long, Decoder]
  end CounterData

  final case class Counter(metricId: MetricID, counter: CounterData) extends MetricElement
      derives Codec.AsObject

  opaque type GaugeData = Json
  object GaugeData:
    def apply(js: Json): GaugeData = js
    extension (gd: GaugeData) def value: Json = gd

    given Encoder[GaugeData] = OpaqueLift.lift[GaugeData, Json, Encoder]
    given Decoder[GaugeData] = OpaqueLift.lift[GaugeData, Json, Decoder]
  end GaugeData

  final case class Gauge(metricId: MetricID, gauge: GaugeData) extends MetricElement derives Codec.AsObject

  final case class MeterData(
    squants: Squants,
    aggregate: Long,
    mean_rate: Frequency,
    m1_rate: Frequency,
    m5_rate: Frequency,
    m15_rate: Frequency
  ) derives Codec.AsObject

  final case class Meter(metricId: MetricID, meter: MeterData) extends MetricElement derives Codec.AsObject

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
  ) derives Codec.AsObject
  final case class Timer(metricId: MetricID, timer: TimerData) extends MetricElement derives Codec.AsObject

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
  ) derives Codec.AsObject

  final case class Histogram(metricId: MetricID, histogram: HistogramData) extends MetricElement
      derives Codec.AsObject
}

final case class Snapshot(
  counters: List[MetricElement.Counter],
  meters: List[MetricElement.Meter],
  timers: List[MetricElement.Timer],
  histograms: List[MetricElement.Histogram],
  gauges: List[MetricElement.Gauge])
    derives Codec.AsObject {
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

  def lookupCount: Map[MetricID, Long] = {
    meters.map(m => m.metricId -> m.meter.aggregate) :::
      timers.map(t => t.metricId -> t.timer.calls) :::
      histograms.map(h => h.metricId -> h.histogram.updates)
  }.toMap

  def sorted: Snapshot = Snapshot(
    counters = counters.sortBy(_.metricId.metricName.age),
    meters = meters.sortBy(_.metricId.metricName.age),
    timers = timers.sortBy(_.metricId.metricName.age),
    histograms = histograms.sortBy(_.metricId.metricName.age),
    gauges = gauges.sortBy(_.metricId.metricName.age)
  )
}

object Snapshot:
  val empty: Snapshot = Snapshot(Nil, Nil, Nil, Nil, Nil)
