package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, SnapshotCategory}
import com.influxdb.client.{InfluxDBClient, WriteOptions}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.localdate

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
            "task" -> sp.taskParams.taskName.value,
            "service" -> sp.serviceName.value,
            "host" -> sp.taskParams.hostName.value,
            "launchDate" -> sp.launchTime.toLocalDate.show
          ) ++ tags // allow override fixed tags

          val counters: List[Point] = snapshot.counters.map(counter =>
            Point
              .measurement(counter.name)
              .time(ts.toInstant, WritePrecision.MS)
              .addTags(tagToAdd.asJava)
              .addTag("category", SnapshotCategory.Counter.name)
              .addField("count", counter.count)) // Long

          val timers: List[Point] = snapshot.timers.map(timer =>
            Point
              .measurement(timer.name)
              .time(ts.toInstant, WritePrecision.MS)
              .addTags(tagToAdd.asJava)
              .addTag("category", SnapshotCategory.Timer.name)
              .addTag("rate_unit", sp.metricParams.rateTimeUnit.name())
              .addTag("duration_unit", sp.metricParams.durationTimeUnit.name())
              .addField("count", timer.count) // Long
              .addField("mean_rate", timer.mean_rate) // Double
              .addField("stddev", sp.metricParams.durationTimeUnit.convert(timer.stddev).toDouble) // Double
              .addField("95%", sp.metricParams.durationTimeUnit.convert(timer.p95).toDouble) // Double
              .addField("99.9%", sp.metricParams.durationTimeUnit.convert(timer.p999).toDouble)) // Double

          val meters: List[Point] = snapshot.meters.map(meter =>
            Point
              .measurement(meter.name)
              .time(ts.toInstant, WritePrecision.MS)
              .addTags(tagToAdd.asJava)
              .addTag("category", SnapshotCategory.Meter.name)
              .addTag("rate_unit", sp.metricParams.rateTimeUnit.name())
              .addField("count", meter.count) // Long
              .addField("mean_rate", meter.mean_rate)) // Double

          val histograms: List[Point] = snapshot.histograms.map(histo =>
            Point
              .measurement(histo.name)
              .time(ts.toInstant, WritePrecision.MS)
              .addTags(tagToAdd.asJava)
              .addTag("category", SnapshotCategory.Histogram.name)
              .addField("count", histo.count) // Long
              .addField("stddev", histo.stddev) // Double
              .addField("95%", histo.p95) // Double
              .addField("99.9%", histo.p999)) // Double

          F.blocking(writer.writePoints((counters ::: timers ::: meters ::: histograms).asJava))
        case _ => F.unit
      }
    } yield event
}
