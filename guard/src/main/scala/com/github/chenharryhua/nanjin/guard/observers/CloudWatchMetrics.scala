package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.*
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.event.{MetricsReport, NJEvent}
import fs2.{INothing, Pipe, Pull, Stream}

import java.time.ZonedDateTime
import java.util.{Date, UUID}
import scala.collection.JavaConverters.*

final private case class MetricKey(
  uuid: UUID,
  hostName: String,
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
        new Dimension().withName("Service").withValue(service),
        new Dimension().withName("Host").withValue(hostName)
      )
      .withMetricName(metricName)
      .withUnit(standardUnit)
      .withTimestamp(Date.from(ts.toInstant))
      .withValue(count)

}

final class CloudWatchMetrics private[observers] (
  namespace: String,
  storageResolution: Int,
  metricFilter: MetricFilter) {
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
            report.serviceInfo.uuid,
            report.serviceParams.taskParams.hostName,
            StandardUnit.Count,
            "CounterCount",
            report.serviceParams.taskParams.appName,
            report.serviceParams.serviceName,
            metricName
          ) -> counter.getCount
        }
        .toMap

      val timers: Map[MetricKey, Long] = mr
        .getTimers(metricFilter)
        .asScala
        .map { case (metricName, counter) =>
          MetricKey(
            report.serviceInfo.uuid,
            report.serviceParams.taskParams.hostName,
            StandardUnit.Count,
            "TimerCount",
            report.serviceParams.taskParams.appName,
            report.serviceParams.serviceName,
            metricName
          ) -> counter.getCount
        }
        .toMap

      (counters ++ timers).foldLeft((last, List.empty[MetricDatum])) { case ((last, mds), (key, count)) =>
        last.get(key) match {
          case Some(old) if count >= old =>
            (last.updated(key, count), key.metricDatum(report.timestamp, count - old) :: mds)
          case None => (last.updated(key, count), key.metricDatum(report.timestamp, count) :: mds)
        }
      }
    }
    res.fold((Map.empty[MetricKey, Long], List.empty[MetricDatum]))(identity)
  }

  def sink[F[_]](implicit F: Async[F]): Pipe[F, NJEvent, INothing] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, INothing, Unit] =
      ss.pull.uncons.flatMap {
        case Some((events, tail)) =>
          val (next, mds) = events.collect { case mr: MetricsReport => mr }
            .foldLeft((Map.empty[MetricKey, Long], List.empty[MetricDatum])) { case ((mk, md), mr) =>
              val (m, d) = buildMetricDatum(mr, last)
              (mk ++ m, md ::: d)
            }

          val publish: F[List[PutMetricDataResult]] =
            mds // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
              .grouped(20)
              .toList
              .traverse(ds =>
                cw.putMetricData(
                  new PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(ds.map(_.withStorageResolution(storageResolution)).asJava)))
          Pull.eval(publish) >> go(cw, tail, next)

        case None => Pull.done
      }

    (ss: Stream[F, NJEvent]) => Stream.resource(CloudWatch[F]).flatMap(cw => go(cw, ss, Map.empty).stream)
  }
}
