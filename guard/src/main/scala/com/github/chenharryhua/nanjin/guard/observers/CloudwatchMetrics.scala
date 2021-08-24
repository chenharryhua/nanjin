package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.event.{MetricsReport, NJEvent}
import fs2.{INothing, Pipe, Pull, Stream}

import java.time.ZonedDateTime
import java.util.Date
import scala.collection.JavaConverters.*

final private case class MetricKey(
  standardUnit: StandardUnit,
  metricType: String,
  task: String,
  service: String,
  metricName: String) {
  def metricDatum(ts: ZonedDateTime, count: Long): MetricDatum = new MetricDatum()
    .withDimensions(
      new Dimension().withName("MetricType").withValue(metricType),
      new Dimension().withName("Task").withValue(task),
      new Dimension().withName("Service").withValue(service)
    )
    .withMetricName(metricName)
    .withUnit(standardUnit)
    .withTimestamp(Date.from(ts.toInstant))
    .withValue(count)
}

final private[observers] class CloudwatchMetrics[F[_]](namespace: String) {
  private def buildMetricDatum(
    report: MetricsReport,
    last: Map[MetricKey, Long]): (Map[MetricKey, Long], List[MetricDatum]) = {
    val res = report.metrics.registry.map { mr =>
      val counters = mr.getCounters.asScala.map { case (metricName, counter) =>
        MetricKey(
          StandardUnit.Count,
          "Count",
          report.serviceParams.taskParams.appName,
          report.serviceParams.serviceName,
          metricName) -> counter.getCount
      }.toMap

      val timers = mr.getTimers.asScala.map { case (metricName, counter) =>
        MetricKey(
          StandardUnit.Count,
          "TimerCount",
          report.serviceParams.taskParams.appName,
          report.serviceParams.serviceName,
          metricName) -> counter.getCount
      }.toMap

      (counters ++ timers).foldLeft((last, List.empty[MetricDatum])) { case ((map, mds), (key, count)) =>
        map.get(key) match {
          case Some(old) =>
            if (count > old)
              (map.updated(key, count), key.metricDatum(report.timestamp, count - old) :: mds)
            else if (count === old)
              (map, mds)
            else
              (map.updated(key, count), key.metricDatum(report.timestamp, count) :: mds)

          case None => (map.updated(key, count), key.metricDatum(report.timestamp, count) :: mds)
        }
      }
    }
    res.fold((Map.empty[MetricKey, Long], List.empty[MetricDatum]))(identity)
  }

  def pipe(implicit F: Async[F]): Pipe[F, NJEvent, INothing] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, INothing, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((evt, tail)) =>
          evt match {
            case mr: MetricsReport =>
              val (map, mds) = buildMetricDatum(mr, last)
              Pull.eval(
                cw.putMetricData(new PutMetricDataRequest().withNamespace(namespace).withMetricData(mds.asJava))) >>
                go(cw, tail, map)
            case _ => go(cw, tail, last)
          }
        case None => Pull.done
      }

    (ss: Stream[F, NJEvent]) => Stream.resource(CloudWatch[F]).flatMap(cw => go(cw, ss, Map.empty).stream)
  }
}
