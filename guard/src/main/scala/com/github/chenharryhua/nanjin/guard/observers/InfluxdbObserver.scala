package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow}
import com.influxdb.client.InfluxDBClient
import org.typelevel.cats.time.instances.localdate
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.influxdb.client.WriteOptions
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import fs2.{Pipe, Stream}

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
          val counters: List[Point] = snapshot.counterMap.map { case (measurement, count) =>
            Point
              .measurement(measurement)
              .time(ts.toInstant, WritePrecision.MS)
              .addField("count", count)
              .addTag("task", sp.taskParams.taskName.value)
              .addTag("service", sp.serviceName.value)
              .addTag("host", sp.taskParams.hostName.value)
              .addTag("launchDate", sp.launchTime.toLocalDate.show)
              .addTags(tags.asJava)
          }.toList
          F.blocking(writer.writePoints(counters.asJava))
        case _ => F.unit
      }
    } yield event
}
