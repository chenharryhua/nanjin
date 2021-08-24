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
  metricName: String,
  storageResolution: Int) {
  def metricDatum(ts: ZonedDateTime, count: Long): MetricDatum =
    new MetricDatum()
      .withDimensions(
        new Dimension().withName("MetricType").withValue(metricType),
        new Dimension().withName("Task").withValue(task),
        new Dimension().withName("Service").withValue(service)
      )
      .withStorageResolution(storageResolution)
      .withMetricName(metricName)
      .withUnit(standardUnit)
      .withTimestamp(Date.from(ts.toInstant))
      .withValue(count)

}

final class CloudWatchMetrics[F[_]](namespace: String, storageResolution: Int, metricFilter: MetricFilter) {
  def withStorageResolution(storageResolution: Int): CloudWatchMetrics[F] = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchMetrics[F](namespace, storageResolution, metricFilter)
  }

  def withMetricFilter(metricFilter: MetricFilter): CloudWatchMetrics[F] =
    new CloudWatchMetrics[F](namespace, storageResolution, metricFilter)

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
            "Count",
            report.serviceParams.taskParams.appName,
            report.serviceParams.serviceName,
            metricName,
            storageResolution) -> counter.getCount
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
            metricName,
            storageResolution) -> counter.getCount
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

  def sink(implicit F: Async[F]): Pipe[F, NJEvent, INothing] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, INothing, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((event, tail)) =>
          event match {
            case mr: MetricsReport =>
              val (next, mds) = buildMetricDatum(mr, last)
              Pull.eval(
                cw.putMetricData(new PutMetricDataRequest().withNamespace(namespace).withMetricData(mds.asJava))) >>
                go(cw, tail, next)
            case _ => go(cw, tail, last)
          }
        case None => Pull.done
      }

    (ss: Stream[F, NJEvent]) => Stream.resource(CloudWatch[F]).flatMap(cw => go(cw, ss, Map.empty).stream)
  }
}

object CloudWatchMetrics {
  def apply[F[_]](namespace: String) = new CloudWatchMetrics[F](namespace, 60, MetricFilter.ALL)
}
