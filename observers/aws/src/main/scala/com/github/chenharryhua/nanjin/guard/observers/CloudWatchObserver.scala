package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.{MetricID, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJDataRateUnit, NJDimensionlessUnit, NJEvent, NJInformationUnit, NJTimeUnit}
import com.github.chenharryhua.nanjin.guard.translators.metricConstants
import com.github.chenharryhua.nanjin.guard.translators.textConstants.*
import fs2.{Pipe, Pull, Stream}
import org.typelevel.cats.time.instances.localdate.*
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataResponse, StandardUnit}

import java.time.Instant
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Sync](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, 60, NJTimeUnit.MILLISECONDS, List.empty)
}

final class CloudWatchObserver[F[_]: Sync](
  client: Resource[F, CloudWatch[F]],
  storageResolution: Int,
  durationUnit: NJTimeUnit,
  fields: List[HistogramField]) {
  private def add(hf: HistogramField): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, durationUnit, hf :: fields)

  def withMin: CloudWatchObserver[F]    = add(HistogramField.Min)
  def withMax: CloudWatchObserver[F]    = add(HistogramField.Max)
  def withMean: CloudWatchObserver[F]   = add(HistogramField.Mean)
  def withStdDev: CloudWatchObserver[F] = add(HistogramField.StdDev)
  def withP50: CloudWatchObserver[F]    = add(HistogramField.P50)
  def withP75: CloudWatchObserver[F]    = add(HistogramField.P75)
  def withP95: CloudWatchObserver[F]    = add(HistogramField.P95)
  def withP98: CloudWatchObserver[F]    = add(HistogramField.P98)
  def withP99: CloudWatchObserver[F]    = add(HistogramField.P99)
  def withP999: CloudWatchObserver[F]   = add(HistogramField.P999)

  /** storageResolution Valid values are 1 and 60. Setting this to 1 specifies this metric as a
    * high-resolution metric, so that CloudWatch stores the metric with sub-minute resolution down to one
    * second. Setting this to 60 specifies this metric as a regular-resolution metric, which CloudWatch stores
    * at 1-minute resolution.
    *
    * if you do not specify it the default of 60 is used.
    */
  def withStorageResolution(storageResolution: Int): CloudWatchObserver[F] = {
    require(
      storageResolution > 0 && storageResolution <= 60,
      s"storageResolution($storageResolution) should be between 1 and 60 inclusively")
    new CloudWatchObserver(client, storageResolution, durationUnit, fields)
  }

  def withDurationUnit(durationUnit: NJTimeUnit): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, durationUnit, fields)

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
      case NJDimensionlessUnit.PERCENT     => StandardUnit.PERCENT
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

  private lazy val interestedHistogramFields: List[HistogramField] = fields.distinct
  private def computeDatum(
    report: MetricReport,
    last: Map[MetricKey, Long]): (List[MetricDatum], Map[MetricKey, Long]) = {

    val timer_histo: List[MetricDatum] = for {
      hf <- interestedHistogramFields
      timer <- report.snapshot.timers
    } yield {
      val (dur, category) = hf.pick(timer)
      MetricKey(
        serviceParams = report.serviceParams,
        id = timer.metricId,
        category = s"${timer.metricId.category.name}_$category",
        standardUnit = toStandardUnit(durationUnit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, durationUnit.from(dur).value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- interestedHistogramFields
      histo <- report.snapshot.histograms
    } yield {
      val (value, category) = hf.pick(histo)
      MetricKey(
        serviceParams = report.serviceParams,
        id = histo.metricId,
        category = s"${histo.metricId.category.name}_$category",
        standardUnit = toStandardUnit(histo.histogram.unit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, value)
    }

    val timer_count: Map[MetricKey, Long] = report.snapshot.timers.map { timer =>
      MetricKey(
        serviceParams = report.serviceParams,
        id = timer.metricId,
        category = s"${timer.metricId.category.name}_count",
        standardUnit = StandardUnit.COUNT,
        storageResolution = storageResolution
      ) -> timer.timer.count
    }.toMap

    val meter_count: Map[MetricKey, Long] = report.snapshot.meters.map { meter =>
      MetricKey(
        serviceParams = report.serviceParams,
        id = meter.metricId,
        category = s"${meter.metricId.category.name}_count",
        standardUnit = toStandardUnit(meter.meter.unit),
        storageResolution = storageResolution
      ) -> meter.meter.count
    }.toMap

    val histogram_count: Map[MetricKey, Long] = report.snapshot.histograms.map { histo =>
      MetricKey(
        serviceParams = report.serviceParams,
        id = histo.metricId,
        category = s"${histo.metricId.category.name}_count",
        standardUnit = StandardUnit.COUNT,
        storageResolution = storageResolution
      ) -> histo.histogram.count
    }.toMap

    val counterKeyMap: Map[MetricKey, Long] = timer_count ++ meter_count ++ histogram_count

    val (counters, lastUpdates) = counterKeyMap.foldLeft((List.empty[MetricDatum], last)) {
      case ((mds, last), (key, count)) =>
        last.get(key) match {
          case Some(old) => // counters of timer/meter increase monotonically.
            (
              key.metricDatum(report.timestamp.toInstant, (count - old).toDouble) :: mds,
              last.updated(key, count))
          case None =>
            (key.metricDatum(report.timestamp.toInstant, count.toDouble) :: mds, last.updated(key, count))
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

final private case class MetricKey(
  serviceParams: ServiceParams,
  id: MetricID,
  category: String,
  standardUnit: StandardUnit,
  storageResolution: Int) {
  def metricDatum(ts: Instant, value: Double): MetricDatum =
    MetricDatum
      .builder()
      .dimensions(
        Dimension.builder().name(CONSTANT_TASK).value(serviceParams.taskParams.taskName).build(),
        Dimension.builder().name(CONSTANT_SERVICE).value(serviceParams.serviceName).build(),
        Dimension.builder().name(CONSTANT_SERVICE_ID).value(serviceParams.serviceId.show).build(),
        Dimension.builder().name(CONSTANT_HOST).value(serviceParams.taskParams.hostName.value).build(),
        Dimension
          .builder()
          .name(metricConstants.METRICS_LAUNCH_TIME)
          .value(serviceParams.launchTime.toLocalDate.show)
          .build(),
        Dimension.builder().name(metricConstants.METRICS_DIGEST).value(id.metricName.digest).build(),
        Dimension.builder().name(CONSTANT_MEASUREMENT).value(id.metricName.measurement).build()
      )
      .metricName(s"${id.metricName.name}($category)")
      .unit(standardUnit)
      .timestamp(ts)
      .value(value)
      .storageResolution(storageResolution)
      .build()
}
