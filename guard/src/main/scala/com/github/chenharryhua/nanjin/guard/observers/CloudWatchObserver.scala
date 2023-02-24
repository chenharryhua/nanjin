package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.*
import com.github.chenharryhua.nanjin.aws.CloudWatchClient
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
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

  private def buildCountDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {

    val keyMap: Map[MetricKey, Long] = report.snapshot.counters.map { counter =>
      MetricKey(
        report.serviceParams.serviceId,
        report.serviceParams.taskParams.hostName.value,
        report.serviceParams.taskParams.taskName.value,
        report.serviceParams.serviceName.value,
        counter.metricName.show,
        report.serviceParams.launchTime.toLocalDate.show
      ) -> counter.count
    }.toMap

    keyMap.foldLeft((List.empty[MetricDatum], last)) { case ((mds, last), (key, count)) =>
      last.get(key) match {
        case Some(old) =>
          if (count > old)
            (
              key.metricDatum(report.timestamp.toInstant, (count - old).toDouble, StandardUnit.Count) :: mds,
              last.updated(key, count))
          else if (count === old) (mds, last)
          else
            (
              key.metricDatum(report.timestamp.toInstant, count.toDouble, StandardUnit.Count) :: mds,
              last.updated(key, count))
        case None =>
          (
            key.metricDatum(report.timestamp.toInstant, count.toDouble, StandardUnit.Count) :: mds,
            last.updated(key, count))
      }
    }
  }

  def observe(namespace: CloudWatchNamespace): Pipe[F, NJEvent, NJEvent] = (es: Stream[F, NJEvent]) => {
    def go(
      cwc: CloudWatchClient[F],
      ss: Stream[F, NJEvent],
      last: Map[MetricKey, Long]): Pull[F, NJEvent, Unit] =
      ss.pull.uncons.flatMap {
        case Some((events, tail)) =>
          val (mds, next) =
            events.collect { case mr: MetricReport => mr }.foldLeft((List.empty[MetricDatum], last)) {
              case ((lmd, last), mr) =>
                val (mds, newLast) = buildCountDatum(mr, last)
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
                      .withNamespace(namespace.value)
                      .withMetricData(ds.map(_.withStorageResolution(storageResolution)).asJava))
                  .attempt)

          Pull.eval(publish) >> Pull.output(events) >> go(cwc, tail, next)

        case None => Pull.done
      }

    Stream.resource(client).flatMap(cw => go(cw, es, Map.empty).stream)
  }
}

final private case class MetricKey(
  serviceId: UUID,
  hostName: String,
  task: String,
  service: String,
  metricName: String,
  launchDate: String) {
  def metricDatum(ts: Instant, value: Double, standardUnit: StandardUnit): MetricDatum =
    new MetricDatum()
      .withDimensions(
        new Dimension().withName(METRICS_TASK).withValue(task),
        new Dimension().withName(METRICS_SERVICE).withValue(service),
        new Dimension().withName(METRICS_HOST).withValue(hostName),
        new Dimension().withName(METRICS_LAUNCH_TIME).withValue(launchDate)
      )
      .withMetricName(metricName)
      .withUnit(standardUnit)
      .withTimestamp(Date.from(ts))
      .withValue(value)
}
