package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricElement, MetricID, Snapshot}
import io.circe.jawn.{decode, parse}
import squants.time.Hertz

import java.time.Duration
import scala.jdk.CollectionConverters.given

enum ScrapeMode:
  case Cheap, Full

final private class ScrapeMetrics(val metricRegistry: MetricRegistry) {
  /*
   *Counters
   */
  private def interpretCounters(sm: java.util.SortedMap[String, Counter]): List[MetricElement.Counter] =
    sm.asScala.iterator.flatMap { case (name, counter) =>
      decode[MetricID](name) match {
        case Left(_)    => None
        case Right(mid) =>
          Some(
            MetricElement.Counter(
              metricId = mid,
              counter = MetricElement.CounterData(counter.getCount)
            ))
      }
    }.toList
  /*
   * meters
   */
  private def interpretMeters(sm: java.util.SortedMap[String, Meter]): List[MetricElement.Meter] =
    sm.asScala.iterator.flatMap { case (name, meter) =>
      decode[MetricID](name) match {
        case Left(_)                                                 => None
        case Right(mid @ MetricID(_, _, Category.Meter(_, squants))) =>
          Some(
            MetricElement.Meter(
              metricId = mid,
              MetricElement.MeterData(
                squants = squants,
                aggregate = meter.getCount,
                mean_rate = Hertz(meter.getMeanRate),
                m1_rate = Hertz(meter.getOneMinuteRate),
                m5_rate = Hertz(meter.getFiveMinuteRate),
                m15_rate = Hertz(meter.getFifteenMinuteRate)
              )
            ))
        case _ => None
      }
    }.toList

  /*
   * Histograms
   */
  private def interpretHistograms(sm: java.util.SortedMap[String, Histogram]): List[MetricElement.Histogram] =
    sm.asScala.iterator.flatMap { case (name, histogram) =>
      decode[MetricID](name) match {
        case Left(_)                                                     => None
        case Right(mid @ MetricID(_, _, Category.Histogram(_, squants))) =>
          val ss = histogram.getSnapshot
          Some(
            MetricElement.Histogram(
              metricId = mid,
              MetricElement.HistogramData(
                squants = squants,
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
            ))
        case _ => None
      }
    }.toList

  /*
   * Timers
   */
  private def interpretTimers(sm: java.util.SortedMap[String, Timer]): List[MetricElement.Timer] =
    sm.asScala.iterator.flatMap { case (name, timer) =>
      decode[MetricID](name) match {
        case Left(_)    => None
        case Right(mid) =>
          val ss = timer.getSnapshot
          Some(
            MetricElement.Timer(
              metricId = mid,
              MetricElement.TimerData(
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
            ))
      }
    }.toList

  /*
   * Gauges
   */
  private def interpretGauges(sm: java.util.SortedMap[String, Gauge[?]]): List[MetricElement.Gauge] =
    sm.asScala.iterator.flatMap { case (name, gauge) =>
      decode[MetricID](name) match {
        case Left(_)    => None
        case Right(mid) =>
          parse(gauge.getValue.toString) match {
            case Right(json) => Some(MetricElement.Gauge(mid, MetricElement.GaugeData(json)))
            case Left(_)     => None
          }
      }
    }.toList

  /*
   * API
   */

  def snapshot[F[_]](mode: ScrapeMode)(using F: Sync[F]): F[Snapshot] =
    mode match {
      case ScrapeMode.Cheap =>
        F.delay {
          val jMeter = metricRegistry.getMeters
          val jTimer = metricRegistry.getTimers
          val jCounter = metricRegistry.getCounters
          val jHistogram = metricRegistry.getHistograms
          Snapshot(
            counters = interpretCounters(jCounter),
            meters = interpretMeters(jMeter),
            timers = interpretTimers(jTimer),
            histograms = interpretHistograms(jHistogram),
            gauges = Nil
          )
        }
      case ScrapeMode.Full => // gauge may run blocking actions
        F.blocking {
          val jMeter = metricRegistry.getMeters
          val jTimer = metricRegistry.getTimers
          val jCounter = metricRegistry.getCounters
          val jHistogram = metricRegistry.getHistograms
          val jGauge = metricRegistry.getGauges
          Snapshot(
            counters = interpretCounters(jCounter),
            meters = interpretMeters(jMeter),
            timers = interpretTimers(jTimer),
            histograms = interpretHistograms(jHistogram),
            gauges = interpretGauges(jGauge)
          )
        }
    }

  // Counts from Meter and Timer metrics only
  def meteredCounts: Map[MetricID, Long] = {
    val meters: java.util.SortedMap[String, Meter] = metricRegistry.getMeters
    val timers: java.util.SortedMap[String, Timer] = metricRegistry.getTimers

    (meters.asScala ++ timers.asScala).flatMap { case (mid, meter) =>
      decode[MetricID](mid).toOption.map(_ -> meter.getCount)
    }.toMap
  }
}
