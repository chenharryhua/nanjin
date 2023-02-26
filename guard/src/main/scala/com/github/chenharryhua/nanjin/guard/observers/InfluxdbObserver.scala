package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, Snapshot}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.client.{InfluxDBClient, WriteOptions}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.localdate

import java.util.concurrent.TimeUnit
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

  /** @param chunkSize
    *   is the maximum number of elements to include in a chunk.
    * @param timeout
    *   is the maximum duration of time to wait before emitting a chunk, even if it doesn't contain chunkSize
    *   elements.
    */
  def observe(chunkSize: Int, timeout: FiniteDuration): Pipe[F, NJEvent, Nothing] = {
    (events: Stream[F, NJEvent]) =>
      Stream
        .bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
          F.blocking(c.close()))
        .flatMap(writer =>
          events
            .groupWithin(chunkSize, timeout)
            .evalMap { chunk =>
              val points = chunk.mapFilter {
                case ar: NJEvent.ActionResultEvent =>
                  val unit = ar.actionParams.serviceParams.metricParams.durationTimeUnit
                  Some(
                    Point
                      .measurement(ar.actionParams.digested.name)
                      .time(ar.timestamp.toInstant, writePrecision)
                      .addTag(METRICS_DIGEST, ar.actionParams.digested.digest)
                      .addTag("done", ar.isDone.show) // for query
                      .addTags(tags.asJava)
                      .addField(unit.name(), unit.convert(ar.took).toDouble) // Double
                  )
                case _ => None
              }.toList.asJava
              F.blocking(writer.writePoints(points))
            }
            .drain)
  }

  // observe metrics instead of action result

  private def dimension(sp: ServiceParams): Map[String, String] =
    Map(
      METRICS_TASK -> sp.taskParams.taskName.value,
      METRICS_SERVICE -> sp.serviceName.value,
      METRICS_HOST -> sp.taskParams.hostName.value,
      METRICS_LAUNCH_TIME -> sp.launchTime.toLocalDate.show
    )

  private def dimension(ms: Snapshot): Map[String, String] =
    Map(
      METRICS_DIGEST -> ms.metricName.digested.digest,
      METRICS_CATEGORY -> ms.metricName.category.entryName
    )

  val observe: Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) =>
    for {
      writer <- Stream.bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
        F.blocking(c.close()))
      event <- events.evalTap {
        case NJEvent.MetricReport(_, sp, ts, snapshot) =>
          val spDimensions = dimension(sp)
          val counters: List[Point] = snapshot.counters.map { counter =>
            val tagToAdd = dimension(counter) ++ spDimensions ++ tags
            Point
              .measurement(counter.metricName.digested.name)
              .time(ts.toInstant, writePrecision)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, counter.count) // Long
          }
          val durationUnit: TimeUnit = sp.metricParams.durationTimeUnit
          val timers: List[Point] = snapshot.timers.map { timer =>
            val tagToAdd = dimension(timer) ++ spDimensions ++ tags
            Point
              .measurement(timer.metricName.digested.name)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addTag(METRICS_DURATION_UNIT, durationUnit.name())
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, timer.count) // Long
              // meter
              .addField(METRICS_MEAN_RATE, timer.mean_rate) // Double
              .addField(METRICS_1_MINUTE_RATE, timer.m1_rate) // Double
              .addField(METRICS_5_MINUTE_RATE, timer.m5_rate) // Double
              .addField(METRICS_15_MINUTE_RATE, timer.m15_rate) // Double
              // histogram
              .addField(METRICS_MIN, durationUnit.convert(timer.min).toDouble) // Double
              .addField(METRICS_MAX, durationUnit.convert(timer.max).toDouble) // Double
              .addField(METRICS_MEAN, durationUnit.convert(timer.mean).toDouble) // Double
              .addField(METRICS_STD_DEV, durationUnit.convert(timer.stddev).toDouble) // Double
              .addField(METRICS_MEDIAN, durationUnit.convert(timer.median).toDouble) // Double
              .addField(METRICS_P75, durationUnit.convert(timer.p75).toDouble) // Double
              .addField(METRICS_P95, durationUnit.convert(timer.p95).toDouble) // Double
              .addField(METRICS_P98, durationUnit.convert(timer.p98).toDouble) // Double
              .addField(METRICS_P99, durationUnit.convert(timer.p99).toDouble) // Double
              .addField(METRICS_P999, durationUnit.convert(timer.p999).toDouble) // Double
          }
          val meters: List[Point] = snapshot.meters.map { meter =>
            val tagToAdd = dimension(meter) ++ spDimensions ++ tags
            Point
              .measurement(meter.metricName.digested.name)
              .time(ts.toInstant, writePrecision)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, meter.count) // Long
              .addField(METRICS_MEAN_RATE, meter.mean_rate) // Double
              .addField(METRICS_1_MINUTE_RATE, meter.m1_rate) // Double
              .addField(METRICS_5_MINUTE_RATE, meter.m5_rate) // Double
              .addField(METRICS_15_MINUTE_RATE, meter.m15_rate) // Double
          }

          val histograms: List[Point] = snapshot.histograms.map { histo =>
            val tagToAdd = dimension(histo) ++ spDimensions ++ tags
            Point
              .measurement(histo.metricName.digested.name)
              .time(ts.toInstant, writePrecision)
              .addTags(tagToAdd.asJava)
              .addField(METRICS_COUNT, histo.count) // Long
              .addField(METRICS_MIN, histo.min) // Double
              .addField(METRICS_MAX, histo.max) // Double
              .addField(METRICS_MEAN, histo.mean) // Double
              .addField(METRICS_STD_DEV, histo.stddev) // Double
              .addField(METRICS_MEDIAN, histo.median) // Double
              .addField(METRICS_P75, histo.p75) // Double
              .addField(METRICS_P95, histo.p95) // Double
              .addField(METRICS_P98, histo.p98) // Double
              .addField(METRICS_P99, histo.p99) // Double
              .addField(METRICS_P999, histo.p999) // Double
          }
          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
