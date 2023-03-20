package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{showStandardUnit, NJEvent, Snapshot}
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
    val name = ar.actionParams.serviceParams.metricParams.durationUnitName
    Some(
      Point
        .measurement(ar.actionParams.name.value)
        .time(ar.timestamp.toInstant, writePrecision)
        .addTag(METRICS_SERVICE_ID, ar.serviceParams.serviceId.show)
        .addTag(METRICS_DIGEST, ar.actionParams.name.digest)
        .addTag("done", ar.isDone.show) // for query
        .addField(name, unit.convert(ar.took)) // Long
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
      METRICS_TASK -> sp.taskParams.taskName.value,
      METRICS_SERVICE -> sp.serviceName.value,
      METRICS_SERVICE_ID -> sp.serviceId.show,
      METRICS_HOST -> sp.taskParams.hostName.value,
      METRICS_LAUNCH_TIME -> sp.launchTime.toLocalDate.show
    )

  private def dimension(ms: Snapshot): Map[String, String] = Map(METRICS_DIGEST -> ms.id.name.digest)

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
              .measurement(timer.id.name.value)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, timer.id.category.tag)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, timer.data.count) // Long
              // meter
              .addField(METRICS_MEAN_RATE, timer.data.mean_rate.toHertz) // Double
              .addField(METRICS_1_MINUTE_RATE, timer.data.m1_rate.toHertz) // Double
              .addField(METRICS_5_MINUTE_RATE, timer.data.m5_rate.toHertz) // Double
              .addField(METRICS_15_MINUTE_RATE, timer.data.m15_rate.toHertz) // Double
              // histogram
              .addField(METRICS_MIN, timer.data.min.toNanos) // Long
              .addField(METRICS_MAX, timer.data.max.toNanos) // Long
              .addField(METRICS_MEAN, timer.data.mean.toNanos) // Long
              .addField(METRICS_STD_DEV, timer.data.stddev.toNanos) // Long
              .addField(METRICS_P50, timer.data.p50.toNanos) // Long
              .addField(METRICS_P75, timer.data.p75.toNanos) // Long
              .addField(METRICS_P95, timer.data.p95.toNanos) // Long
              .addField(METRICS_P98, timer.data.p98.toNanos) // Long
              .addField(METRICS_P99, timer.data.p99.toNanos) // Long
              .addField(METRICS_P999, timer.data.p999.toNanos) // Long
          }
          val meters: List[Point] = snapshot.meters.map { meter =>
            val tagToAdd = dimension(meter) ++ spDimensions ++ tags
            Point
              .measurement(meter.id.name.value)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, meter.id.category.tag)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, meter.data.count) // Long
              // meter
              .addField(METRICS_MEAN_RATE, meter.data.mean_rate.toHertz) // Double
              .addField(METRICS_1_MINUTE_RATE, meter.data.m1_rate.toHertz) // Double
              .addField(METRICS_5_MINUTE_RATE, meter.data.m5_rate.toHertz) // Double
              .addField(METRICS_15_MINUTE_RATE, meter.data.m15_rate.toHertz) // Double
          }

          val counters: List[Point] = snapshot.counters.map { counter =>
            val tagToAdd = dimension(counter) ++ spDimensions ++ tags
            Point
              .measurement(counter.id.name.value)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, counter.id.category.tag)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, counter.count) // Long
          }

          val histograms: List[Point] = snapshot.histograms.map { histo =>
            val tagToAdd = dimension(histo) ++ spDimensions ++ tags
            val unitName = s"(${histo.data.unit.show})"
            Point
              .measurement(histo.id.name.value)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_CATEGORY, histo.id.category.tag)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, histo.data.count) // Long
              .addField(METRICS_MIN + unitName, histo.data.min) // Long
              .addField(METRICS_MAX + unitName, histo.data.max) // Long
              .addField(METRICS_MEAN + unitName, histo.data.mean) // Double
              .addField(METRICS_STD_DEV + unitName, histo.data.stddev) // Double
              .addField(METRICS_P50 + unitName, histo.data.p50) // Double
              .addField(METRICS_P75 + unitName, histo.data.p75) // Double
              .addField(METRICS_P95 + unitName, histo.data.p95) // Double
              .addField(METRICS_P98 + unitName, histo.data.p98) // Double
              .addField(METRICS_P99 + unitName, histo.data.p99) // Double
              .addField(METRICS_P999 + unitName, histo.data.p999) // Double
          }
          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
