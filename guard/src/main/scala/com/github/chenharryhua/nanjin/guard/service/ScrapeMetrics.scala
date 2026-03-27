package com.github.chenharryhua.nanjin.guard.service

import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricElement, MetricID, Snapshot}
import io.circe.jawn.{decode, parse}
import squants.time.Hertz

import java.time.Duration
import scala.jdk.CollectionConverters.given

enum ScrapeMode:
  case Cheap, Full

final private class ScrapeMetrics(val metricRegistry: MetricRegistry) extends AnyVal {
  /*
   *Counters
   */
  private def counters: List[MetricElement.Counter] =
    metricRegistry.getCounters.asScala.iterator.flatMap { case (name, counter) =>
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
  private def meters: List[MetricElement.Meter] =
    metricRegistry.getMeters.asScala.iterator.flatMap { case (name, meter) =>
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
  private def histograms: List[MetricElement.Histogram] =
    metricRegistry.getHistograms.asScala.iterator.flatMap { case (name, histogram) =>
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
  private def timers: List[MetricElement.Timer] =
    metricRegistry.getTimers.asScala.iterator.flatMap { case (name, timer) =>
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
  private def gauges(mode: ScrapeMode): List[MetricElement.Gauge] =
    mode match {
      case ScrapeMode.Cheap => Nil
      case ScrapeMode.Full  =>
        metricRegistry.getGauges.asScala.iterator.flatMap { case (name, gauge) =>
          decode[MetricID](name) match {
            case Left(_)    => None
            case Right(mid) =>
              parse(gauge.getValue.toString) match {
                case Right(json) => Some(MetricElement.Gauge(mid, MetricElement.GaugeData(json)))
                case Left(_)     => None
              }
          }
        }.toList
    }

  /*
   * API
   */

  def resetCounter(): Unit =
    metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  def snapshot(mode: ScrapeMode): Snapshot =
    Snapshot(
      counters = counters,
      meters = meters,
      timers = timers,
      histograms = histograms,
      gauges = gauges(mode)
    )

  def meterCounters: Map[MetricID, Long] = {
    val mc: Map[MetricID, Long] =
      metricRegistry.getMeters.asScala
        .flatMap { case (mid, meter) =>
          decode[MetricID](mid).toOption.map(_ -> meter.getCount)
        }.toMap

    val tc: Map[MetricID, Long] =
      metricRegistry.getTimers.asScala
        .flatMap { case (mid, timer) =>
          decode[MetricID](mid).toOption.map(_ -> timer.getCount)
        }.toMap
    mc ++ tc
  }
}
