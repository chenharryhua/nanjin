package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, Snapshot}
import com.github.chenharryhua.nanjin.guard.translators.{
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

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

object InfluxdbObserver {

  /** The default write precision for InfluxDB is nanosecond (ns). When a new database is created in InfluxDB,
    * the default write precision is set to nanosecond unless otherwise specified
    */
  def apply[F[_]: Async](client: F[InfluxDBClient]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, identity, WritePrecision.NS, Map.empty[String, String])
}

final class InfluxdbObserver[F[_]](
  client: F[InfluxDBClient],
  writeOptions: Endo[WriteOptions.Builder],
  writePrecision: WritePrecision,
  tags: Map[String, String])(implicit F: Async[F])
    extends localdate {
  def withWriteOptions(writeOptions: Endo[WriteOptions.Builder]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, tags)

  def withWritePrecision(wp: WritePrecision): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, wp, tags)

  def addTag(key: String, value: String): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, tags + (key -> value))

  def addTags(tagsToAdd: Map[String, String]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, writePrecision, tags ++ tagsToAdd)

  private def defaultTransform(ar: NJEvent.ActionResultEvent): Option[Point] = {
    val unit = ar.actionParams.serviceParams.metricParams.durationTimeUnit
    Some(
      Point
        .measurement(ar.actionParams.metricId.metricName.measurement)
        .time(ar.timestamp.toInstant, writePrecision)
        .addTag(CONSTANT_SERVICE_ID, ar.serviceParams.serviceId.show)
        .addTag(METRICS_DIGEST, ar.actionParams.metricId.metricName.digest)
        .addTag("done", NJEvent.isActionDone(ar).show) // for query
        .addTags(tags.asJava)
        .addField(ar.actionParams.metricId.metricName.value, unit.convert(ar.took)) // Long
    )
  }

  /** @param chunkSize
    *   is the maximum number of elements to include in a chunk.
    * @param timeout
    *   is the maximum duration of time to wait before emitting a chunk, even if it doesn't contain chunkSize
    *   elements.
    */
  def observe(
    chunkSize: Int,
    timeout: FiniteDuration,
    f: NJEvent.ActionResultEvent => Option[Point]): Pipe[F, NJEvent, Nothing] = {
    (events: Stream[F, NJEvent]) =>
      Stream
        .bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
          F.blocking(c.close()))
        .flatMap { writer =>
          events
            .groupWithin(chunkSize, timeout)
            .evalMap { chunk =>
              val points: List[Point] = chunk.mapFilter {
                case ar: NJEvent.ActionResultEvent => f(ar).map(_.addTags(tags.asJava))
                case _                             => None
              }.toList
              F.blocking(writer.writePoints(points.asJava))
            }
            .drain
        }
  }

  def observe(chunkSize: Int, timeout: FiniteDuration): Pipe[F, NJEvent, Nothing] =
    observe(chunkSize, timeout, defaultTransform)

  /** InfluxDB tag key constraints: Tag keys must be strings. Tag keys must be non-empty. Tag keys must not
    * contain any control characters (e.g., '\n', '\r', '\t'). Tag keys must not contain commas or spaces. Tag
    * keys must not start with an underscore ('_'), as these are reserved for system tags. Tag keys must not
    * exceed 256 bytes in length.
    */

  private def dimension(sp: ServiceParams): Map[String, String] =
    Map(
      CONSTANT_TASK -> sp.taskParams.taskName,
      CONSTANT_SERVICE -> sp.serviceName,
      CONSTANT_SERVICE_ID -> sp.serviceId.show,
      CONSTANT_HOST -> sp.taskParams.hostName.value,
      METRICS_LAUNCH_TIME -> sp.launchTime.toLocalDate.show
    )

  private def dimension(ms: Snapshot): Map[String, String] =
    Map(METRICS_DIGEST -> ms.metricId.metricName.digest, METRICS_NAME -> ms.metricId.metricName.value)

  val observe: Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) =>
    for {
      writer <- Stream.bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
        F.blocking(c.close()))
      event <- events.evalTap {
        case NJEvent.MetricReport(_, sp, ts, snapshot) =>
          val spDimensions: Map[String, String] = dimension(sp)
          val timers: List[Point] = snapshot.timers.map { timer =>
            val tagToAdd = dimension(timer) ++ spDimensions ++ tags
            Point
              .measurement(timer.metricId.metricName.measurement)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, timer.metricId.category.name)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, timer.timer.count) // Long
              // meter
              .addField(METRICS_MEAN_RATE, timer.timer.mean_rate.toHertz) // Double
              .addField(METRICS_1_MINUTE_RATE, timer.timer.m1_rate.toHertz) // Double
              .addField(METRICS_5_MINUTE_RATE, timer.timer.m5_rate.toHertz) // Double
              .addField(METRICS_15_MINUTE_RATE, timer.timer.m15_rate.toHertz) // Double
              // histogram
              .addField(METRICS_MIN, timer.timer.min.toNanos) // Long
              .addField(METRICS_MAX, timer.timer.max.toNanos) // Long
              .addField(METRICS_MEAN, timer.timer.mean.toNanos) // Long
              .addField(METRICS_STD_DEV, timer.timer.stddev.toNanos) // Long
              .addField(METRICS_P50, timer.timer.p50.toNanos) // Long
              .addField(METRICS_P75, timer.timer.p75.toNanos) // Long
              .addField(METRICS_P95, timer.timer.p95.toNanos) // Long
              .addField(METRICS_P98, timer.timer.p98.toNanos) // Long
              .addField(METRICS_P99, timer.timer.p99.toNanos) // Long
              .addField(METRICS_P999, timer.timer.p999.toNanos) // Long
          }
          val meters: List[Point] = snapshot.meters.map { meter =>
            val tagToAdd = dimension(meter) ++ spDimensions ++ tags
            Point
              .measurement(meter.metricId.metricName.measurement)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, meter.metricId.category.name)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, meter.meter.count) // Long
              // meter
              .addField(METRICS_MEAN_RATE, meter.meter.mean_rate.toHertz) // Double
              .addField(METRICS_1_MINUTE_RATE, meter.meter.m1_rate.toHertz) // Double
              .addField(METRICS_5_MINUTE_RATE, meter.meter.m5_rate.toHertz) // Double
              .addField(METRICS_15_MINUTE_RATE, meter.meter.m15_rate.toHertz) // Double
          }

          val counters: List[Point] = snapshot.counters.map { counter =>
            val tagToAdd = dimension(counter) ++ spDimensions ++ tags
            Point
              .measurement(counter.metricId.metricName.measurement)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, counter.metricId.category.name)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, counter.count) // Long
          }

          val histograms: List[Point] = snapshot.histograms.map { histo =>
            val tagToAdd = dimension(histo) ++ spDimensions ++ tags
            val unitName = s"(${histo.histogram.unitShow})"
            Point
              .measurement(histo.metricId.metricName.measurement)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, histo.metricId.category.name)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, histo.histogram.count) // Long
              .addField(METRICS_MIN + unitName, histo.histogram.min) // Long
              .addField(METRICS_MAX + unitName, histo.histogram.max) // Long
              .addField(METRICS_MEAN + unitName, histo.histogram.mean) // Double
              .addField(METRICS_STD_DEV + unitName, histo.histogram.stddev) // Double
              .addField(METRICS_P50 + unitName, histo.histogram.p50) // Double
              .addField(METRICS_P75 + unitName, histo.histogram.p75) // Double
              .addField(METRICS_P95 + unitName, histo.histogram.p95) // Double
              .addField(METRICS_P98 + unitName, histo.histogram.p98) // Double
              .addField(METRICS_P99 + unitName, histo.histogram.p99) // Double
              .addField(METRICS_P999 + unitName, histo.histogram.p999) // Double
          }
          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
