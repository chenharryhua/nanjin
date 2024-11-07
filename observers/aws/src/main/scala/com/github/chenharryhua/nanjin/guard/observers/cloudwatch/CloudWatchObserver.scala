package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.{MetricID, MetricLabel, MetricName}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJEvent, Normalized, UnitNormalization}
import com.github.chenharryhua.nanjin.guard.translator.textConstants
import fs2.{Pipe, Pull, Stream}
import io.scalaland.chimney.dsl.*
import software.amazon.awssdk.services.cloudwatch.model.{
  Dimension,
  MetricDatum,
  PutMetricDataResponse,
  StandardUnit
}

import java.time.Instant
import java.util.UUID
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

object CloudWatchTimeUnit {
  val MICROSECONDS: NJTimeUnit.MICROSECONDS.type = NJTimeUnit.MICROSECONDS
  val MILLISECONDS: NJTimeUnit.MILLISECONDS.type = NJTimeUnit.MILLISECONDS
  val SECONDS: NJTimeUnit.SECONDS.type           = NJTimeUnit.SECONDS
}

final class CloudWatchObserver[F[_]: Sync](
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

  private def toStandardUnit(mu: MeasurementUnit): StandardUnit =
    mu match {
      case NJTimeUnit.SECONDS              => StandardUnit.SECONDS
      case NJTimeUnit.MILLISECONDS         => StandardUnit.MILLISECONDS
      case NJTimeUnit.MICROSECONDS         => StandardUnit.MICROSECONDS
      case NJInformationUnit.BYTES         => StandardUnit.BYTES
      case NJInformationUnit.KILOBYTES     => StandardUnit.KILOBYTES
      case NJInformationUnit.MEGABYTES     => StandardUnit.MEGABYTES
      case NJInformationUnit.GIGABYTES     => StandardUnit.GIGABYTES
      case NJInformationUnit.TERABYTES     => StandardUnit.TERABYTES
      case NJInformationUnit.BITS          => StandardUnit.BITS
      case NJInformationUnit.KILOBITS      => StandardUnit.KILOBITS
      case NJInformationUnit.MEGABITS      => StandardUnit.MEGABITS
      case NJInformationUnit.GIGABITS      => StandardUnit.GIGABITS
      case NJInformationUnit.TERABITS      => StandardUnit.TERABITS
      case NJDimensionlessUnit.COUNT       => StandardUnit.COUNT
      case NJDataRateUnit.BYTES_SECOND     => StandardUnit.BYTES_SECOND
      case NJDataRateUnit.KILOBYTES_SECOND => StandardUnit.KILOBYTES_SECOND
      case NJDataRateUnit.MEGABYTES_SECOND => StandardUnit.MEGABYTES_SECOND
      case NJDataRateUnit.GIGABYTES_SECOND => StandardUnit.GIGABYTES_SECOND
      case NJDataRateUnit.TERABYTES_SECOND => StandardUnit.TERABYTES_SECOND
      case NJDataRateUnit.BITS_SECOND      => StandardUnit.BITS_SECOND
      case NJDataRateUnit.KILOBITS_SECOND  => StandardUnit.KILOBITS_SECOND
      case NJDataRateUnit.MEGABITS_SECOND  => StandardUnit.MEGABITS_SECOND
      case NJDataRateUnit.GIGABITS_SECOND  => StandardUnit.GIGABITS_SECOND
      case NJDataRateUnit.TERABITS_SECOND  => StandardUnit.TERABITS_SECOND

      case _ => StandardUnit.NONE
    }

  private lazy val interestedHistogramFields: List[HistogramField] =
    histogramBuilder(new HistogramFieldBuilder(Nil)).build

  private def computeDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {
    val db = dimensionBuilder(new DimensionBuilder(report.serviceParams, Map.empty))

    val unitNormalization: UnitNormalization =
      unitBuilder(
        new UnitBuilder(
          UnitNormalization(
            timeUnit = CloudWatchTimeUnit.MILLISECONDS,
            infoUnit = None,
            rateUnit = None))).build

    val timer_histo: List[MetricDatum] = for {
      hf <- interestedHistogramFields
      timer <- report.snapshot.timers
    } yield {
      val (dur, category) = hf.pick(timer)
      MetricKey(
        serviceId = report.serviceParams.serviceId,
        metricLabel = timer.metricId.metricLabel,
        metricName = s"${timer.metricId.metricName.name}_$category",
        standardUnit = toStandardUnit(unitNormalization.timeUnit),
        storageResolution = storageResolution
      ).metricDatum(db)(report.timestamp.toInstant, unitNormalization.normalize(dur).value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- interestedHistogramFields
      histo <- report.snapshot.histograms
    } yield {
      val (value, category)      = hf.pick(histo)
      val Normalized(data, unit) = unitNormalization.normalize(histo.histogram.unit, value)
      MetricKey(
        serviceId = report.serviceParams.serviceId,
        metricLabel = histo.metricId.metricLabel,
        metricName = s"${histo.metricId.metricName.name}_$category",
        standardUnit = toStandardUnit(unit),
        storageResolution = storageResolution
      ).metricDatum(db)(report.timestamp.toInstant, data)
    }

    val timer_calls: Map[MetricKey, Long] = report.snapshot.timers.map { timer =>
      MetricKey(
        serviceId = report.serviceParams.serviceId,
        metricLabel = timer.metricId.metricLabel,
        metricName = timer.metricId.metricName.name,
        standardUnit = StandardUnit.COUNT,
        storageResolution = storageResolution
      ) -> timer.timer.calls
    }.toMap

    val meter_sum: Map[MetricKey, Long] = report.snapshot.meters.map { meter =>
      val Normalized(data, unit) = unitNormalization.normalize(meter.meter.unit, meter.meter.sum)
      MetricKey(
        serviceId = report.serviceParams.serviceId,
        metricLabel = meter.metricId.metricLabel,
        metricName = meter.metricId.metricName.name,
        standardUnit = toStandardUnit(unit),
        storageResolution = storageResolution
      ) -> data.toLong
    }.toMap

    val histogram_updates: Map[MetricKey, Long] = report.snapshot.histograms.map { histo =>
      MetricKey(
        serviceId = report.serviceParams.serviceId,
        metricLabel = histo.metricId.metricLabel,
        metricName = histo.metricId.metricName.name,
        standardUnit = StandardUnit.COUNT,
        storageResolution = storageResolution
      ) -> histo.histogram.updates
    }.toMap

    val counterKeyMap: Map[MetricKey, Long] = timer_calls ++ meter_sum ++ histogram_updates

    val (counters, lastUpdates) = counterKeyMap.foldLeft((List.empty[MetricDatum], last)) {
      case ((mds, newLast), (key, count)) =>
        newLast.get(key) match {
          // counters of timer/meter increase monotonically.
          // however, service may be crushed and restart
          // such that counters are reset to zero.
          case Some(old) =>
            val nc = if (count > old) count - old else 0
            (key.metricDatum(db)(report.timestamp.toInstant, nc.toDouble) :: mds, newLast.updated(key, count))
          case None =>
            (
              key.metricDatum(db)(report.timestamp.toInstant, count.toDouble) :: mds,
              newLast.updated(key, count))
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
}

final private case class StableMetricID(
  metricLabel: MetricLabel,
  metricName: MetricName
)

private object StableMetricID {
  def apply(id: MetricID): StableMetricID =
    id.transformInto[StableMetricID]
}

final private case class MetricKey(
  serviceId: UUID,
  metricLabel: MetricLabel,
  metricName: String,
  standardUnit: StandardUnit,
  storageResolution: Int) {
  def metricDatum(dimensionBuilder: DimensionBuilder)(ts: Instant, value: Double): MetricDatum =
    MetricDatum
      .builder()
      .dimensions(dimensionBuilder.build)
      .dimensions(
        Dimension.builder().name(textConstants.CONSTANT_MEASUREMENT).value(metricLabel.measurement).build(),
        Dimension.builder().name(textConstants.CONSTANT_DIGEST).value(metricLabel.digest).build(),
        Dimension.builder().name(textConstants.CONSTANT_LABEL).value(metricLabel.label).build()
      )
      .metricName(metricName)
      .unit(standardUnit)
      .timestamp(ts)
      .value(value)
      .storageResolution(storageResolution)
      .build()
}
