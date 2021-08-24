package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.codahale.metrics.MetricFilter
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
  def metricDatum(ts: ZonedDateTime, count: Long): MetricDatum =
    new MetricDatum()
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

final class CloudWatchMetrics(namespace: String, storageResolution: Int, metricFilter: MetricFilter) {
  def withStorageResolution(storageResolution: Int): CloudWatchMetrics = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchMetrics(namespace, storageResolution, metricFilter)
  }

  def withMetricFilter(metricFilter: MetricFilter): CloudWatchMetrics =
    new CloudWatchMetrics(namespace, storageResolution, metricFilter)

  private def buildMetricDatum(
    report: MetricsReport,
    last: Map[MetricKey, Long]): (Map[MetricKey, Long], List[MetricDatum]) = {

    val res: Option[(Map[MetricKey, Long], List[MetricDatum])] = report.metrics.registry.map { mr =>
      val counters: Map[MetricKey, Long] = mr
        .getCounters(metricFilter)
        .asScala
        .map { case (metricName, counter) =>
          MetricKey(
            StandardUnit.Count,
            "CounterCount",
            report.serviceParams.taskParams.appName,
            report.serviceParams.serviceName,
            metricName) -> counter.getCount
        }
        .toMap

      val timers: Map[MetricKey, Long] = mr
        .getTimers(metricFilter)
        .asScala
        .map { case (metricName, counter) =>
          MetricKey(
            StandardUnit.Count,
            "TimerCount",
            report.serviceParams.taskParams.appName,
            report.serviceParams.serviceName,
            metricName) -> counter.getCount
        }
        .toMap

      (counters ++ timers).foldLeft((last, List.empty[MetricDatum])) { case ((map, mds), (key, count)) =>
        map.get(key) match {
          case Some(old) if count >= old =>
            (map.updated(key, count), key.metricDatum(report.timestamp, count - old) :: mds)
          case None => (map.updated(key, count), key.metricDatum(report.timestamp, count) :: mds)
        }
      }
    }
    res.fold((Map.empty[MetricKey, Long], List.empty[MetricDatum]))(identity)
  }

  def sink[F[_]](implicit F: Async[F]): Pipe[F, NJEvent, INothing] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, INothing, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((event, tail)) =>
          event match {
            case mr: MetricsReport =>
              val (next, mds) = buildMetricDatum(mr, last)
              Pull.eval(
                cw.putMetricData(
                  new PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(mds.map(_.withStorageResolution(storageResolution)).asJava))) >>
                go(cw, tail, next)
            case _ => go(cw, tail, last)
          }
        case None => Pull.done
      }

    (ss: Stream[F, NJEvent]) => Stream.resource(CloudWatch[F]).flatMap(cw => go(cw, ss, Map.empty).stream)
  }
}

object CloudWatchMetrics {
  def apply(namespace: String) = new CloudWatchMetrics(namespace, 60, MetricFilter.ALL)
}
