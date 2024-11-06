package com.github.chenharryhua.nanjin.guard.observers.influxdb

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, Snapshot}
import com.github.chenharryhua.nanjin.guard.translator.metricConstants
import com.github.chenharryhua.nanjin.guard.translator.textConstants.{
  CONSTANT_HOST,
  CONSTANT_SERVICE,
  CONSTANT_SERVICE_ID,
  CONSTANT_TASK
}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.client.{InfluxDBClient, WriteOptions}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.localdate

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

object InfluxdbObserver {

  /** The default write precision for InfluxDB is nanosecond (ns). When a new database is created in InfluxDB,
    * the default write precision is set to nanosecond unless otherwise specified
    */
  def apply[F[_]: Async](client: F[InfluxDBClient]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](
      Resource.make(client)(c => Async[F].blocking(c.close())),
      identity,
      WritePrecision.NS,
      TimeUnit.MILLISECONDS,
      Map.empty[String, String])

  def apply[F[_]: Async](client: => InfluxDBClient): InfluxdbObserver[F] =
    apply[F](Async[F].blocking(client))
}

final class InfluxdbObserver[F[_]](
  client: Resource[F, InfluxDBClient],
  writeOptions: Endo[WriteOptions.Builder],
  writePrecision: WritePrecision,
  durationUnit: TimeUnit,
  tags: Map[String, String])(implicit F: Async[F])
    extends localdate {
  def withWriteOptions(writeOptions: Endo[WriteOptions.Builder]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, durationUnit, tags)

  def withWritePrecision(wp: WritePrecision): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, wp, durationUnit, tags)

  def withDurationUnit(durationUnit: TimeUnit): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, durationUnit, tags)

  def addTag(key: String, value: String): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, durationUnit, tags + (key -> value))

  def addTags(tagsToAdd: Map[String, String]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, durationUnit, tags ++ tagsToAdd)

  /** InfluxDB tag key constraints: Tag keys must be strings. Tag keys must be non-empty. Tag keys must not
    * contain any control characters (e.g., '\n', '\r', '\t'). Tag keys must not contain commas or spaces. Tag
    * keys must not start with an underscore ('_'), as these are reserved for system tags. Tag keys must not
    * exceed 256 bytes in length.
    */

  private def dimension(sp: ServiceParams): Map[String, String] =
    Map(
      CONSTANT_TASK -> sp.taskName.value,
      CONSTANT_SERVICE -> sp.serviceName.value,
      CONSTANT_SERVICE_ID -> sp.serviceId.show,
      CONSTANT_HOST -> sp.hostName.value,
      metricConstants.METRICS_LAUNCH_TIME -> sp.zerothTick.zonedLaunchTime.toLocalDate.show
    )

  private def dimension(ms: Snapshot): Map[String, String] =
    Map(
      metricConstants.METRICS_DIGEST -> ms.metricId.metricLabel.digest,
      metricConstants.METRICS_NAME -> ms.metricId.metricLabel.label)

  val observe: Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) =>
    for {
      writer <- Stream.resource(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))
      event <- events.evalTap {
        case NJEvent.MetricReport(_, sp, snapshot, timestamp) =>
          val spDimensions: Map[String, String] = dimension(sp)
          val timers: List[Point] = snapshot.timers.map { timer =>
            val tagToAdd = dimension(timer) ++ spDimensions ++ tags
            Point
              .measurement(timer.metricId.metricLabel.measurement)
              .time(timestamp.toInstant, writePrecision)
              .addTag(metricConstants.METRICS_CATEGORY, timer.metricId.tag)
              .addTags(tagToAdd.asJava)
              .addField(metricConstants.METRICS_COUNT, timer.timer.calls) // Long
              // meter
              .addField(metricConstants.METRICS_MEAN_RATE, timer.timer.mean_rate.toHertz) // Double
              .addField(metricConstants.METRICS_1_MINUTE_RATE, timer.timer.m1_rate.toHertz) // Double
              .addField(metricConstants.METRICS_5_MINUTE_RATE, timer.timer.m5_rate.toHertz) // Double
              .addField(metricConstants.METRICS_15_MINUTE_RATE, timer.timer.m15_rate.toHertz) // Double
              // histogram
              .addField(metricConstants.METRICS_MIN, timer.timer.min.toNanos) // Long
              .addField(metricConstants.METRICS_MAX, timer.timer.max.toNanos) // Long
              .addField(metricConstants.METRICS_MEAN, timer.timer.mean.toNanos) // Long
              .addField(metricConstants.METRICS_STD_DEV, timer.timer.stddev.toNanos) // Long
              .addField(metricConstants.METRICS_P50, timer.timer.p50.toNanos) // Long
              .addField(metricConstants.METRICS_P75, timer.timer.p75.toNanos) // Long
              .addField(metricConstants.METRICS_P95, timer.timer.p95.toNanos) // Long
              .addField(metricConstants.METRICS_P98, timer.timer.p98.toNanos) // Long
              .addField(metricConstants.METRICS_P99, timer.timer.p99.toNanos) // Long
              .addField(metricConstants.METRICS_P999, timer.timer.p999.toNanos) // Long
          }
          val meters: List[Point] = snapshot.meters.map { meter =>
            val tagToAdd = dimension(meter) ++ spDimensions ++ tags
            Point
              .measurement(meter.metricId.metricLabel.measurement)
              .time(timestamp.toInstant, writePrecision)
              .addTag(metricConstants.METRICS_CATEGORY, meter.metricId.tag)
              .addTags(tagToAdd.asJava)
              .addField(metricConstants.METRICS_COUNT, meter.meter.sum) // Long
              // meter
              .addField(metricConstants.METRICS_MEAN_RATE, meter.meter.mean_rate.toHertz) // Double
              .addField(metricConstants.METRICS_1_MINUTE_RATE, meter.meter.m1_rate.toHertz) // Double
              .addField(metricConstants.METRICS_5_MINUTE_RATE, meter.meter.m5_rate.toHertz) // Double
              .addField(metricConstants.METRICS_15_MINUTE_RATE, meter.meter.m15_rate.toHertz) // Double
          }

          val counters: List[Point] = snapshot.counters.map { counter =>
            val tagToAdd = dimension(counter) ++ spDimensions ++ tags
            Point
              .measurement(counter.metricId.metricLabel.measurement)
              .time(timestamp.toInstant, writePrecision)
              .addTag(metricConstants.METRICS_CATEGORY, counter.metricId.tag)
              .addTags(tagToAdd.asJava)
              .addField(metricConstants.METRICS_COUNT, counter.count) // Long
          }

          val histograms: List[Point] = snapshot.histograms.map { histo =>
            val tagToAdd = dimension(histo) ++ spDimensions ++ tags
            val unitName = s"(${histo.histogram.unit.symbol})"
            Point
              .measurement(histo.metricId.metricLabel.measurement)
              .time(timestamp.toInstant, writePrecision)
              .addTag(metricConstants.METRICS_CATEGORY, histo.metricId.tag)
              .addTags(tagToAdd.asJava)
              .addField(metricConstants.METRICS_COUNT, histo.histogram.updates) // Long
              .addField(metricConstants.METRICS_MIN + unitName, histo.histogram.min) // Long
              .addField(metricConstants.METRICS_MAX + unitName, histo.histogram.max) // Long
              .addField(metricConstants.METRICS_MEAN + unitName, histo.histogram.mean) // Double
              .addField(metricConstants.METRICS_STD_DEV + unitName, histo.histogram.stddev) // Double
              .addField(metricConstants.METRICS_P50 + unitName, histo.histogram.p50) // Double
              .addField(metricConstants.METRICS_P75 + unitName, histo.histogram.p75) // Double
              .addField(metricConstants.METRICS_P95 + unitName, histo.histogram.p95) // Double
              .addField(metricConstants.METRICS_P98 + unitName, histo.histogram.p98) // Double
              .addField(metricConstants.METRICS_P99 + unitName, histo.histogram.p99) // Double
              .addField(metricConstants.METRICS_P999 + unitName, histo.histogram.p999) // Double
          }
          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
