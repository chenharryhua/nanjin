package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.event.{MetricsReport, NJEvent}
import fs2.{INothing, Pipe, Pull, Stream}

import java.util.Date
import scala.collection.JavaConverters.*

class CloudwatchMetrics[F[_]] {
  private def buildMetricDatum(report: MetricsReport): List[MetricDatum] =
    report.metrics.registry.traverse { mr =>
      mr.getCounters().asScala.toList.map { case (n, c) =>
        new MetricDatum()
          .withDimensions(
            new Dimension().withName("MetricType").withValue("Counter"),
            new Dimension().withName("Service").withValue(report.serviceParams.serviceName)
          )
          .withMetricName(n)
          .withUnit(StandardUnit.Count)
          .withTimestamp(Date.from(report.timestamp.toInstant))
          .withValue(c.getCount)
          .withStorageResolution(1)
      }
    }.flatten

  def pipe(implicit F: Async[F]): Pipe[F, NJEvent, INothing] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[String, Long]): Pull[F, INothing, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((mr, tail)) => Pull.eval(cw.putMetricData(new PutMetricDataRequest)) >> go(cw, tail, last)
        case None             => Pull.done
      }

    (ss: Stream[F, NJEvent]) =>
      Stream
        .resource(CloudWatch[F])
        .flatMap { cw =>
          ss.collect { case mr: MetricsReport => mr }.evalMap { mr =>
            val putMetricDataRequest = new PutMetricDataRequest()
              .withNamespace(mr.serviceParams.taskParams.appName)
              .withMetricData(buildMetricDatum(mr).asJava)
            cw.putMetricData(putMetricDataRequest)
          }
        }
        .drain
  }
}
