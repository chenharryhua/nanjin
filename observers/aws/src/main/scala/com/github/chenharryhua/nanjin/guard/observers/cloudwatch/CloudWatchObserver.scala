package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.Endo
import cats.effect.kernel.{Async, Concurrent, Resource}
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.{
  Attribute,
  MetricID,
  MetricLabel,
  ServiceId,
  ServiceParams,
  Squants
}
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsReport
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricsReportData, Snapshot, Timestamp}
import fs2.{Pipe, Stream}
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum, StandardUnit}
import squants.time

import java.time.ZoneId
import java.util
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.JavaDurationOps

object CloudWatchObserver {
  def apply[F[_]: Async](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client = client,
      storageResolution = 60,
      histogramBuilder = identity,
      dimensionBuilder = identity
    )
}

final class CloudWatchObserver[F[_]: Async] private (
  client: Resource[F, CloudWatch[F]],
  storageResolution: Int,
  histogramBuilder: Endo[HistogramFieldBuilder],
  dimensionBuilder: Endo[DimensionBuilder]) {
  private val F = Concurrent[F]

  def withHighStorageResolution: CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, 1, histogramBuilder, dimensionBuilder)

  def includeHistogram(f: Endo[HistogramFieldBuilder]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, f, dimensionBuilder)

  def includeDimensions(f: Endo[DimensionBuilder]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, histogramBuilder, f)

  private val histogramB: HistogramFieldBuilder = histogramBuilder(new HistogramFieldBuilder(false, Nil))

  private def computeDatum(report: MetricsReport, lookup: Map[MetricID, Long]): List[MetricDatum] = {

    val timer_histo: List[MetricDatum] = for {
      hf <- histogramB.build
      timer <- report.snapshot.timers
    } yield {
      val (dur, category) = hf.pick(timer)
      val (su, value) = CloudWatchTimeUnit.toStandardUnit(
        squants = Squants(time.Microseconds.symbol, time.Time.name),
        data = time.Time(dur.toScala).to(time.Microseconds)
      )
      MetricKey(
        timestamp = report.timestamp,
        serviceParams = report.serviceParams,
        metricLabel = timer.metricId.metricLabel,
        metricName = s"${timer.metricId.metricName.name}_$category",
        standardUnit = su
      ).metricDatum(value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- histogramB.build
      histo <- report.snapshot.histograms
    } yield {
      val (value, category) = hf.pick(histo)
      val (su, data) = CloudWatchTimeUnit.toStandardUnit(squants = histo.histogram.squants, data = value)
      MetricKey(
        timestamp = report.timestamp,
        serviceParams = report.serviceParams,
        metricLabel = histo.metricId.metricLabel,
        metricName = s"${histo.metricId.metricName.name}_$category",
        standardUnit = su
      ).metricDatum(data)
    }

    val timer_count: List[MetricDatum] =
      report.snapshot.timers.map { timer =>
        val calls: Long = timer.timer.calls
        val delta: Long = lookup.get(timer.metricId).fold(calls)(calls - _)
        MetricKey(
          timestamp = report.timestamp,
          serviceParams = report.serviceParams,
          metricLabel = timer.metricId.metricLabel,
          metricName = timer.metricId.metricName.name,
          standardUnit = StandardUnit.COUNT
        ) -> delta.toDouble
      }.groupBy(_._1).toList.map { case (key, lst) => key.metricDatum(lst.map(_._2).sum) }

    val meter_count: List[MetricDatum] =
      report.snapshot.meters.map { meter =>
        val aggregate: Long = meter.meter.aggregate
        val value: Long = lookup.get(meter.metricId).fold(aggregate)(aggregate - _)
        val (su, data) =
          CloudWatchTimeUnit.toStandardUnit(squants = meter.meter.squants, data = value.toDouble)
        MetricKey(
          timestamp = report.timestamp,
          serviceParams = report.serviceParams,
          metricLabel = meter.metricId.metricLabel,
          metricName = meter.metricId.metricName.name,
          standardUnit = su
        ) -> data
      }.groupBy(_._1).toList.map { case (key, lst) => key.metricDatum(lst.map(_._2).sum) }

    val histogram_count: List[MetricDatum] =
      if (histogramB.includeUpdate)
        report.snapshot.histograms.map { histo =>
          val updates: Long = histo.histogram.updates
          val delta: Long = lookup.get(histo.metricId).fold(updates)(updates - _)
          MetricKey(
            timestamp = report.timestamp,
            serviceParams = report.serviceParams,
            metricLabel = histo.metricId.metricLabel,
            metricName = histo.metricId.metricName.name,
            standardUnit = StandardUnit.COUNT
          ) -> delta.toDouble
        }.groupBy(_._1).toList.map { case (key, lst) => key.metricDatum(lst.map(_._2).sum) }
      else Nil

    timer_count ::: meter_count ::: histogram_count ::: timer_histo ::: histograms
  }

  def observe(namespace: CloudWatchNamespace): Pipe[F, Event, Event] = (events: Stream[F, Event]) => {
    def publish(cwc: CloudWatch[F], mds: List[MetricDatum]): F[Unit] =
      mds // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
        .grouped(20)
        .toList
        .traverse(md => cwc.putMetricData(_.namespace(namespace.value).metricData(md.asJava)).attempt)
        .void

    for {
      cwc <- Stream.resource(client)
      // indexed by ServiceID and MetricName's uuid
      lookup <- Stream.eval(F.ref(Map.empty[ServiceId, Map[MetricID, Long]]))
      event <- events.evalTap {
        case mr @ MetricsReport(MetricsReportData.Index.Periodic(_), sp, snapshot, _) =>
          lookup.getAndUpdate(_.updated(sp.serviceId, snapshot.lookupCount)).flatMap { last =>
            val data = computeDatum(mr, last.getOrElse(sp.serviceId, Snapshot.empty.lookupCount))
            publish(cwc, data)
          }
        case Event.ServiceStop(serviceParams, _, _) =>
          lookup.update(_.removed(serviceParams.serviceId))
        case _ => F.unit
      }
    } yield event
  }

  def scrape(namespace: CloudWatchNamespace, zoneId: ZoneId, f: Policy.type => Policy)(
    getMetricsReport: Tick => F[MetricsReport]): Stream[F, Event] =
    tickStream.tickScheduled(zoneId, f).evalMap(getMetricsReport).through(observe(namespace))

  private case class MetricKey(
    timestamp: Timestamp,
    serviceParams: ServiceParams,
    metricLabel: MetricLabel,
    metricName: String,
    standardUnit: StandardUnit) {

    private val permanent: Map[String, String] =
      Map(Attribute(metricLabel.label).textEntry.toPair, Attribute(metricLabel.domain).textEntry.toPair)

    private val dimensions: util.List[Dimension] =
      dimensionBuilder(new DimensionBuilder(serviceParams, permanent)).build

    def metricDatum(value: Double): MetricDatum =
      MetricDatum
        .builder()
        .dimensions(dimensions)
        .metricName(metricName)
        .unit(standardUnit)
        .timestamp(timestamp.value.toInstant)
        .value(value)
        .storageResolution(storageResolution)
        .build()
  }
}
