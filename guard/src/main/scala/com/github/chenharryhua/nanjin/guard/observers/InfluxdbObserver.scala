package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, SnapshotCategory}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.client.{InfluxDBClient, WriteOptions}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.localdate

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

object InfluxdbObserver {
  def apply[F[_]: Sync](client: F[InfluxDBClient]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, identity, Map.empty[String, String])
}

final class InfluxdbObserver[F[_]](
  client: F[InfluxDBClient],
  writeOptions: Endo[WriteOptions.Builder],
  tags: Map[String, String])(implicit F: Sync[F])
    extends localdate {
  def withWriteOptions(writeOptions: Endo[WriteOptions.Builder]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, tags)

  def addTag(key: String, value: String): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, tags + (key -> value))

  val observe: Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) =>
    for {
      writer <- Stream.bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
        F.blocking(c.close()))
      event <- events.evalTap {
        case NJEvent.MetricReport(_, sp, ts, snapshot) =>
          val tagToAdd: Map[String, String] = Map(
            METRICS_TASK -> sp.taskParams.taskName.value,
            METRICS_SERVICE -> sp.serviceName.value,
            METRICS_SERVICE_ID -> sp.serviceId.show,
            METRICS_HOST -> sp.taskParams.hostName.value,
            METRICS_LAUNCH_TIME -> sp.launchTime.toLocalDate.show
          ) ++ tags // allow override fixed tags

          val counters: List[Point] = snapshot.counters.map(counter =>
            Point
              .measurement(counter.name)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tagToAdd.asJava)
              .addTag(METRICS_CATEGORY, SnapshotCategory.Counter.name)
              .addField(METRICS_COUNT, counter.count) // Long
          )
          val durationUnit: TimeUnit = sp.metricParams.durationTimeUnit
          val timers: List[Point] = snapshot.timers.map(timer =>
            Point
              .measurement(timer.name)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tagToAdd.asJava)
              .addTag(METRICS_CATEGORY, SnapshotCategory.Timer.name)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addTag(METRICS_DURATION_UNIT, durationUnit.name())
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
          )
          val meters: List[Point] = snapshot.meters.map(meter =>
            Point
              .measurement(meter.name)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tagToAdd.asJava)
              .addTag(METRICS_CATEGORY, SnapshotCategory.Meter.name)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addField(METRICS_COUNT, meter.count) // Long
              .addField(METRICS_MEAN_RATE, meter.mean_rate) // Double
              .addField(METRICS_1_MINUTE_RATE, meter.m1_rate) // Double
              .addField(METRICS_5_MINUTE_RATE, meter.m5_rate) // Double
              .addField(METRICS_15_MINUTE_RATE, meter.m15_rate) // Double
          )

          val histograms: List[Point] = snapshot.histograms.map(histo =>
            Point
              .measurement(histo.name)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tagToAdd.asJava)
              .addTag(METRICS_CATEGORY, SnapshotCategory.Histogram.name)
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
          )
          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
