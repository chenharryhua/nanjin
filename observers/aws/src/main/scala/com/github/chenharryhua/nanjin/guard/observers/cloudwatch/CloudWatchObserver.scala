package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.{MetricLabel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, Normalized, UnitNormalization}
import com.github.chenharryhua.nanjin.guard.translator.textConstants
import fs2.{Pipe, Pull, Stream}
import software.amazon.awssdk.services.cloudwatch.model.{
  Dimension,
  MetricDatum,
  PutMetricDataResponse,
  StandardUnit
}

import java.time.Instant
import java.util
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Sync](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client = client,
      storageResolution = 60,
      histogramBuilder = identity,
      unitBuilder = identity,
      dimensionBuilder = identity
    )
}

final class CloudWatchObserver[F[_]: Sync] private (
  client: Resource[F, CloudWatch[F]],
  storageResolution: Int,
  histogramBuilder: Endo[HistogramFieldBuilder],
  unitBuilder: Endo[UnitBuilder],
  dimensionBuilder: Endo[DimensionBuilder]) {

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

  private def computeDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {

    val timer_histo: List[MetricDatum] = for {
      hf <- histogramB.build
      timer <- report.snapshot.timers
    } yield {
      val (dur, category) = hf.pick(timer)
      MetricKey(
        serviceParams = report.serviceParams,
        metricLabel = timer.metricId.metricLabel,
        metricName = s"${timer.metricId.metricName.name}_$category",
        standardUnit = CloudWatchTimeUnit.toStandardUnit(unitNormalization.timeUnit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, unitNormalization.normalize(dur).value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- histogramB.build
      histo <- report.snapshot.histograms
    } yield {
      val (value, category)      = hf.pick(histo)
      val Normalized(data, unit) = unitNormalization.normalize(histo.histogram.unit, value)
      MetricKey(
        serviceParams = report.serviceParams,
        metricLabel = histo.metricId.metricLabel,
        metricName = s"${histo.metricId.metricName.name}_$category",
        standardUnit = CloudWatchTimeUnit.toStandardUnit(unit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, data)
    }

    val timer_calls: Map[MetricKey, Long] =
      report.snapshot.timers.map { timer =>
        MetricKey(
          serviceParams = report.serviceParams,
          metricLabel = timer.metricId.metricLabel,
          metricName = timer.metricId.metricName.name,
          standardUnit = StandardUnit.COUNT,
          storageResolution = storageResolution
        ) -> timer.timer.calls
      }.toMap

    val meter_sum: Map[MetricKey, Long] =
      report.snapshot.meters.map { meter =>
        val Normalized(data, unit) = unitNormalization.normalize(meter.meter.unit, meter.meter.sum)
        MetricKey(
          serviceParams = report.serviceParams,
          metricLabel = meter.metricId.metricLabel,
          metricName = meter.metricId.metricName.name,
          standardUnit = CloudWatchTimeUnit.toStandardUnit(unit),
          storageResolution = storageResolution
        ) -> data.toLong
      }.toMap

    val histogram_updates: Map[MetricKey, Long] =
      if (histogramB.includeUpdate)
        report.snapshot.histograms.map { histo =>
          MetricKey(
            serviceParams = report.serviceParams,
            metricLabel = histo.metricId.metricLabel,
            metricName = histo.metricId.metricName.name,
            standardUnit = StandardUnit.COUNT,
            storageResolution = storageResolution
          ) -> histo.histogram.updates
        }.toMap
      else Map.empty

    val counterKeyMap: Map[MetricKey, Long] = timer_calls ++ meter_sum ++ histogram_updates

    val (counters, lastUpdates) = counterKeyMap.foldLeft((List.empty[MetricDatum], last)) {
      case ((mds, newLast), (key, count)) =>
        newLast.get(key) match {
          // counters of timer/meter increase monotonically.
          // however, service may be crushed and restart
          // such that counters are reset to zero.
          case Some(old) =>
            val nc = if (count > old) count - old else 0
            (key.metricDatum(report.timestamp.toInstant, nc.toDouble) :: mds, newLast.updated(key, count))
          case None =>
            (key.metricDatum(report.timestamp.toInstant, count.toDouble) :: mds, newLast.updated(key, count))
        }
    }
    (counters ::: timer_histo ::: histograms, lastUpdates)
  }

  def observe(namespace: CloudWatchNamespace): Pipe[F, NJEvent, NJEvent] = (es: Stream[F, NJEvent]) => {
    def go(cwc: CloudWatch[F], ss: Stream[F, NJEvent], last: Map[MetricKey, Long]): Pull[F, NJEvent, Unit] =
      ss.pull.uncons.flatMap {
        case Some((events, tail)) =>
          val (mds, next) =
            events.collect { case mr: MetricReport => mr }.foldLeft((List.empty[MetricDatum], last)) {
              case ((lmd, last), mr) =>
                val (mds, newLast) = computeDatum(mr, last)
                (mds ::: lmd, newLast)
            }

          val publish: F[List[Either[Throwable, PutMetricDataResponse]]] =
            mds // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
              .grouped(20)
              .toList
              .traverse(ds => cwc.putMetricData(_.namespace(namespace.value).metricData(ds.asJava)).attempt)

          Pull.eval(publish) >> Pull.output(events) >> go(cwc, tail, next)

        case None => Pull.done
      }

    Stream.resource(client).flatMap(cw => go(cw, es, Map.empty).stream)
  }

  private case class MetricKey(
    serviceParams: ServiceParams,
    metricLabel: MetricLabel,
    metricName: String,
    standardUnit: StandardUnit,
    storageResolution: Int) {

    private val permanent: Map[String, String] = Map(
      textConstants.CONSTANT_LABEL -> metricLabel.label,
      textConstants.CONSTANT_MEASUREMENT -> metricLabel.measurement.value)

    private val dimensions: util.List[Dimension] =
      dimensionBuilder(new DimensionBuilder(serviceParams, permanent)).build

    def metricDatum(ts: Instant, value: Double): MetricDatum =
      MetricDatum
        .builder()
        .dimensions(dimensions)
        .metricName(metricName)
        .unit(standardUnit)
        .timestamp(ts)
        .value(value)
        .storageResolution(storageResolution)
        .build()
  }
}
