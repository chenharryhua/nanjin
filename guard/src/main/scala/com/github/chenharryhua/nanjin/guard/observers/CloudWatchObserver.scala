package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.*
import com.github.chenharryhua.nanjin.aws.CloudWatchClient
import com.github.chenharryhua.nanjin.guard.event.{MetricReport, NJEvent}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.cats.time.instances.localdate

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Sync](client: Resource[F, CloudWatchClient[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, 60)

}

final class CloudWatchObserver[F[_]: Sync](client: Resource[F, CloudWatchClient[F]], storageResolution: Int)
    extends localdate {

  def withStorageResolution(storageResolution: Int): CloudWatchObserver[F] = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchObserver(client, storageResolution)
  }

  private def buildMetricDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {

    val keyMap: Map[MetricKey, Long] = report.snapshot.counterMap.map { case (metricName, counter) =>
      MetricKey(
        report.serviceParams.serviceID,
        report.serviceParams.taskParams.hostName.value,
        StandardUnit.Count,
        report.serviceParams.taskParams.taskName.value,
        report.serviceParams.serviceName.value,
        metricName,
        report.serviceParams.launchTime.toLocalDate.show
      ) -> counter
    }

    keyMap.foldLeft((List.empty[MetricDatum], last)) { case ((mds, last), (key, count)) =>
      last.get(key) match {
        case Some(old) =>
          if (count > old) (key.metricDatum(report.timestamp.toInstant, count - old) :: mds, last.updated(key, count))
          else if (count === old) (mds, last)
          else (key.metricDatum(report.timestamp.toInstant, count) :: mds, last.updated(key, count))
        case None =>
          (key.metricDatum(report.timestamp.toInstant, count) :: mds, last.updated(key, count))
      }
    }
  }

  def observe(namespace: String): Pipe[F, NJEvent, NJEvent] = (es: Stream[F, NJEvent]) => {
    def go(cwc: CloudWatchClient[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, NJEvent, Unit] =
      ss.pull.uncons.flatMap {
        case Some((events, tail)) =>
          val (mds, next) = events.collect { case mr: MetricReport => mr }.foldLeft((List.empty[MetricDatum], last)) {
            case ((lmd, last), mr) =>
              val (mds, newLast) = buildMetricDatum(mr, last)
              (mds ::: lmd, newLast)
          }

          val publish: F[List[Either[Throwable, PutMetricDataResult]]] =
            mds // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
              .grouped(20)
              .toList
              .traverse(ds =>
                cwc
                  .putMetricData(
                    new PutMetricDataRequest()
                      .withNamespace(namespace)
                      .withMetricData(ds.map(_.withStorageResolution(storageResolution)).asJava))
                  .attempt)

          Pull.eval(publish) >> Pull.output(events) >> go(cwc, tail, next)

        case None => Pull.done
      }

    Stream.resource(client).flatMap(cw => go(cw, es, Map.empty).stream)
  }
}

final private case class MetricKey(
  uuid: UUID,
  hostName: String,
  standardUnit: StandardUnit,
  task: String,
  service: String,
  metricName: String,
  launchDate: String) {
  def metricDatum(ts: Instant, count: Long): MetricDatum =
    new MetricDatum()
      .withDimensions(
        new Dimension().withName("Task").withValue(task),
        new Dimension().withName("Service").withValue(service),
        new Dimension().withName("Host").withValue(hostName),
        new Dimension().withName("LaunchDate").withValue(launchDate)
      )
      .withMetricName(metricName)
      .withUnit(standardUnit)
      .withTimestamp(Date.from(ts))
      .withValue(count)
}
