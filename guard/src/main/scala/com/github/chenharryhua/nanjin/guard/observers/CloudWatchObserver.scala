package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.*
import com.github.chenharryhua.nanjin.aws.CloudWatchClient
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MetricName, NJEvent}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.cats.time.instances.localdate.*

import java.time.{Duration, Instant}
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Sync](client: Resource[F, CloudWatchClient[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, 60)

}

final class CloudWatchObserver[F[_]: Sync](client: Resource[F, CloudWatchClient[F]], storageResolution: Int) {

  def withStorageResolution(storageResolution: Int): CloudWatchObserver[F] = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchObserver(client, storageResolution)
  }

  private def unitConversion(duration: Duration, timeUnit: TimeUnit): (Long, StandardUnit) =
    timeUnit match {
      case TimeUnit.NANOSECONDS  => (TimeUnit.MICROSECONDS.convert(duration), StandardUnit.Microseconds)
      case TimeUnit.MICROSECONDS => (TimeUnit.MICROSECONDS.convert(duration), StandardUnit.Microseconds)
      case TimeUnit.MILLISECONDS => (TimeUnit.MILLISECONDS.convert(duration), StandardUnit.Milliseconds)
      case TimeUnit.SECONDS      => (TimeUnit.SECONDS.convert(duration), StandardUnit.Seconds)
      case TimeUnit.MINUTES      => (TimeUnit.SECONDS.convert(duration), StandardUnit.Seconds)
      case TimeUnit.HOURS        => (TimeUnit.SECONDS.convert(duration), StandardUnit.Seconds)
      case TimeUnit.DAYS         => (TimeUnit.SECONDS.convert(duration), StandardUnit.Seconds)
    }

  private def buildCountDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {

    val timers: List[MetricDatum] = report.snapshot.timers.map { timer =>
      val (p95, unit) = unitConversion(timer.p95, report.serviceParams.metricParams.durationTimeUnit)
      MetricKey(report.serviceParams, timer.metricName)
        .metricDatum(report.timestamp.toInstant, p95.toDouble, unit)
    }

    val counterKeyMap: Map[MetricKey, Long] = report.snapshot.counters.map { counter =>
      MetricKey(report.serviceParams, counter.metricName) -> counter.count
    }.toMap

    val (counters, lastUpdates) = counterKeyMap.foldLeft((List.empty[MetricDatum], last)) {
      case ((mds, last), (key, count)) =>
        last.get(key) match {
          case Some(old) =>
            if (count > old)
              (
                key.metricDatum(
                  report.timestamp.toInstant,
                  (count - old).toDouble,
                  StandardUnit.Count) :: mds,
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
    (counters ::: timers, lastUpdates)
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

final private case class MetricKey(serviceParams: ServiceParams, metricName: MetricName) {
  def metricDatum(ts: Instant, value: Double, standardUnit: StandardUnit): MetricDatum =
    new MetricDatum()
      .withDimensions(
        new Dimension().withName(METRICS_TASK).withValue(serviceParams.taskParams.taskName.value),
        new Dimension().withName(METRICS_SERVICE).withValue(serviceParams.serviceName.value),
        new Dimension().withName(METRICS_HOST).withValue(serviceParams.taskParams.hostName.value),
        new Dimension().withName(METRICS_LAUNCH_TIME).withValue(serviceParams.launchTime.toLocalDate.show),
        new Dimension().withName(METRICS_DIGEST).withValue(metricName.digested.digest),
        new Dimension().withName(METRICS_CATEGORY).withValue(metricName.category.entryName)
      )
      .withMetricName(metricName.digested.name)
      .withUnit(standardUnit)
      .withTimestamp(Date.from(ts))
      .withValue(value)
}
