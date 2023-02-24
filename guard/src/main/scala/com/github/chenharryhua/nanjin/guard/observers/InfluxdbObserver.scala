package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.client.{InfluxDBClient, WriteOptions}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.localdate

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

object InfluxdbObserver {
  def apply[F[_]: Async](client: F[InfluxDBClient]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, identity, Map.empty[String, String])
}

final class InfluxdbObserver[F[_]](
  client: F[InfluxDBClient],
  writeOptions: Endo[WriteOptions.Builder],
  tags: Map[String, String])(implicit F: Async[F])
    extends localdate {
  def withWriteOptions(writeOptions: Endo[WriteOptions.Builder]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, tags)

  def addTag(key: String, value: String): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, tags + (key -> value))

  def addTags(tagsToAdd: Map[String, String]): InfluxdbObserver[F] =
    new InfluxdbObserver[F](client, writeOptions, tags ++ tagsToAdd)

  val observeMetrics: Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) =>
    for {
      writer <- Stream.bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
        F.blocking(c.close()))
      event <- events.evalTap {
        case NJEvent.MetricReport(_, sp, ts, snapshot) =>
          val counters: List[Point] = snapshot.counters.map(counter =>
            Point
              .measurement(counter.metricName.show)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tags.asJava)
              .addField(METRICS_COUNT, counter.count) // Long
          )
          val durationUnit: TimeUnit = sp.metricParams.durationTimeUnit
          val timers: List[Point] = snapshot.timers.map(timer =>
            Point
              .measurement(timer.metricName.show)
              .time(ts.toInstant, WritePrecision.NS)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addTag(METRICS_DURATION_UNIT, durationUnit.name())
              .addTags(tags.asJava)
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
              .measurement(meter.metricName.show)
              .time(ts.toInstant, WritePrecision.NS)
              .addTag(METRICS_RATE_UNIT, sp.metricParams.rateTimeUnit.name())
              .addTags(tags.asJava)
              .addField(METRICS_COUNT, meter.count) // Long
              .addField(METRICS_MEAN_RATE, meter.mean_rate) // Double
              .addField(METRICS_1_MINUTE_RATE, meter.m1_rate) // Double
              .addField(METRICS_5_MINUTE_RATE, meter.m5_rate) // Double
              .addField(METRICS_15_MINUTE_RATE, meter.m15_rate) // Double
          )

          val histograms: List[Point] = snapshot.histograms.map(histo =>
            Point
              .measurement(histo.metricName.show)
              .time(ts.toInstant, WritePrecision.NS)
              .addTags(tags.asJava)
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

  def observe(chunkSize: Int, period: FiniteDuration): Pipe[F, NJEvent, Nothing] = {
    (events: Stream[F, NJEvent]) =>
      Stream
        .bracket(client.map(_.makeWriteApi(writeOptions(WriteOptions.builder()).build())))(c =>
          F.blocking(c.close()))
        .flatMap(writer =>
          events
            .groupWithin(chunkSize, period)
            .evalMap { chunk => // tried parEvalMap no performance gain
              val points = chunk.mapFilter {
                case ar: NJEvent.ActionResultEvent =>
                  val unit = ar.actionParams.serviceParams.metricParams.durationTimeUnit
                  Some(
                    Point
                      .measurement(ar.actionParams.digested.name)
                      .time(ar.timestamp.toInstant, WritePrecision.NS)
                      .addTag(METRICS_DIGEST, ar.actionParams.digested.digest)
                      .addTags(tags.asJava)
                      .addField(unit.name(), unit.convert(ar.took).toDouble) // Double
                      .addField("is_succ", ar.isSucc) // Boolean
                  )
                case _ => None
              }.toList.asJava
              F.blocking(writer.writePoints(points))
            }
            .drain)
  }
}
