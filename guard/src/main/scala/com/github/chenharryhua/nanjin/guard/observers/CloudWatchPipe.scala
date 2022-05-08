package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.event.{MetricReport, NJEvent}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.cats.time.instances.localdate

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

object cloudwatch {
  def apply[F[_]: Sync](client: Resource[F, CloudWatch[F]], namespace: String): CloudWatchPipe[F] =
    new CloudWatchPipe[F](client, namespace, 60)

  def apply[F[_]: Sync](namespace: String): CloudWatchPipe[F] =
    apply[F](CloudWatch[F], namespace)
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

final class CloudWatchPipe[F[_]] private[observers] (
  client: Resource[F, CloudWatch[F]],
  namespace: String,
  storageResolution: Int)(implicit F: Sync[F])
    extends Pipe[F, NJEvent, NJEvent] with localdate {

  def withStorageResolution(storageResolution: Int): CloudWatchPipe[F] = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchPipe(client, namespace, storageResolution)
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

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] = {
    def go(cw: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, NJEvent, Unit] =
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
                cw.putMetricData(
                  new PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(ds.map(_.withStorageResolution(storageResolution)).asJava))
                  .attempt)

          Pull.eval(publish) >> Pull.output(events) >> go(cw, tail, next)

        case None => Pull.done
      }

    Stream.resource(client).flatMap(cw => go(cw, es, Map.empty).stream)
  }
}
