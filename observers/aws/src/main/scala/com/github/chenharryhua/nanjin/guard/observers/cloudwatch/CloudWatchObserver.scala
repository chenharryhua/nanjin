package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.Endo
import cats.effect.kernel.{Concurrent, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.{MetricLabel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{
  Event,
  MetricIndex,
  MetricSnapshot,
  Normalized,
  UnitNormalization
}
import com.github.chenharryhua.nanjin.guard.translator.textConstants
import fs2.{Pipe, Stream}
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum, StandardUnit}

import java.time.Instant
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Concurrent](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client = client,
      storageResolution = 60,
      histogramBuilder = identity,
      unitBuilder = identity,
      dimensionBuilder = identity
    )
}

final class CloudWatchObserver[F[_]: Concurrent] private (
  client: Resource[F, CloudWatch[F]],
  storageResolution: Int,
  histogramBuilder: Endo[HistogramFieldBuilder],
  unitBuilder: Endo[UnitBuilder],
  dimensionBuilder: Endo[DimensionBuilder]) {
  private val F = Concurrent[F]

  def withHighStorageResolution: CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, 1, histogramBuilder, unitBuilder, dimensionBuilder)

  def includeHistogram(f: Endo[HistogramFieldBuilder]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, f, unitBuilder, dimensionBuilder)

  def includeDimensions(f: Endo[DimensionBuilder]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, histogramBuilder, unitBuilder, f)

  def unifyMeasurementUnit(f: Endo[UnitBuilder]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, histogramBuilder, f, dimensionBuilder)

  private val histogramB: HistogramFieldBuilder = histogramBuilder(new HistogramFieldBuilder(false, Nil))

  private val unitNormalization: UnitNormalization =
    unitBuilder(new UnitBuilder(
      UnitNormalization(timeUnit = CloudWatchTimeUnit.MILLISECONDS, infoUnit = None, rateUnit = None))).build

  private def computeDatum(report: MetricReport, lookup: Map[UUID, Long]): List[MetricDatum] = {

    val timer_histo: List[MetricDatum] = for {
      hf <- histogramB.build
      timer <- report.snapshot.timers
    } yield {
      val (dur, category) = hf.pick(timer)
      MetricKey(
        timestamp = report.timestamp.toInstant,
        serviceParams = report.serviceParams,
        metricLabel = timer.metricId.metricLabel,
        metricName = s"${timer.metricId.metricName.name}_$category",
        standardUnit = CloudWatchTimeUnit.toStandardUnit(unitNormalization.timeUnit)
      ).metricDatum(unitNormalization.normalize(dur).value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- histogramB.build
      histo <- report.snapshot.histograms
    } yield {
      val (value, category)      = hf.pick(histo)
      val Normalized(data, unit) = unitNormalization.normalize(histo.histogram.unit, value)
      MetricKey(
        timestamp = report.timestamp.toInstant,
        serviceParams = report.serviceParams,
        metricLabel = histo.metricId.metricLabel,
        metricName = s"${histo.metricId.metricName.name}_$category",
        standardUnit = CloudWatchTimeUnit.toStandardUnit(unit)
      ).metricDatum(data)
    }

    val timer_count: List[MetricDatum] =
      report.snapshot.timers.map { timer =>
        val calls: Long = timer.timer.calls
        val delta: Long = lookup.get(timer.metricId.metricName.uuid).fold(calls)(calls - _)
        MetricKey(
          timestamp = report.timestamp.toInstant,
          serviceParams = report.serviceParams,
          metricLabel = timer.metricId.metricLabel,
          metricName = timer.metricId.metricName.name,
          standardUnit = StandardUnit.COUNT
        ) -> delta.toDouble
      }.groupBy(_._1).toList.map { case (key, lst) => key.metricDatum(lst.map(_._2).sum) }

    val meter_count: List[MetricDatum] =
      report.snapshot.meters.map { meter =>
        val aggregate: Long = meter.meter.aggregate
        val value: Long     = lookup.get(meter.metricId.metricName.uuid).fold(aggregate)(aggregate - _)
        val Normalized(delta, unit) = unitNormalization.normalize(meter.meter.unit, value)
        MetricKey(
          timestamp = report.timestamp.toInstant,
          serviceParams = report.serviceParams,
          metricLabel = meter.metricId.metricLabel,
          metricName = meter.metricId.metricName.name,
          standardUnit = CloudWatchTimeUnit.toStandardUnit(unit)
        ) -> delta
      }.groupBy(_._1).toList.map { case (key, lst) => key.metricDatum(lst.map(_._2).sum) }

    val histogram_count: List[MetricDatum] =
      if (histogramB.includeUpdate)
        report.snapshot.histograms.map { histo =>
          val updates: Long = histo.histogram.updates
          val delta: Long   = lookup.get(histo.metricId.metricName.uuid).fold(updates)(updates - _)
          MetricKey(
            timestamp = report.timestamp.toInstant,
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
      lookup <- Stream.eval(F.ref(Map.empty[UUID, Map[UUID, Long]]))
      event <- events.evalTap {
        case mr @ MetricReport(MetricIndex.Periodic(_), sp, snapshot, _) =>
          lookup.getAndUpdate(_.updated(sp.serviceId, snapshot.lookupCount)).flatMap { last =>
            val data = computeDatum(mr, last.getOrElse(sp.serviceId, MetricSnapshot.empty.lookupCount))
            publish(cwc, data)
          }
        case Event.ServiceStop(serviceParams, _, _) =>
          lookup.update(_.removed(serviceParams.serviceId))
        case _ => F.unit
      }
    } yield event
  }

  private case class MetricKey(
    timestamp: Instant,
    serviceParams: ServiceParams,
    metricLabel: MetricLabel,
    metricName: String,
    standardUnit: StandardUnit) {

    private val permanent: Map[String, String] = Map(
      textConstants.CONSTANT_LABEL -> metricLabel.label,
      textConstants.CONSTANT_MEASUREMENT -> metricLabel.measurement.value)

    private val dimensions: util.List[Dimension] =
      dimensionBuilder(new DimensionBuilder(serviceParams, permanent)).build

    def metricDatum(value: Double): MetricDatum =
      MetricDatum
        .builder()
        .dimensions(dimensions)
        .metricName(metricName)
        .unit(standardUnit)
        .timestamp(timestamp)
        .value(value)
        .storageResolution(storageResolution)
        .build()
  }
}
