package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.common.aws.CloudWatchNamespace
import com.github.chenharryhua.nanjin.guard.config.{MetricID, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJEvent, Normalized, UnitNormalization}
import com.github.chenharryhua.nanjin.guard.translator.metricConstants
import com.github.chenharryhua.nanjin.guard.translator.textConstants.*
import fs2.{Pipe, Pull, Stream}
import io.scalaland.chimney.dsl.*
import monocle.syntax.all.*
import org.typelevel.cats.time.instances.localdate.*
import software.amazon.awssdk.services.cloudwatch.model.{
  Dimension,
  MetricDatum,
  PutMetricDataResponse,
  StandardUnit
}

import java.time.Instant
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Sync](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client = client,
      storageResolution = 60,
      UnitNormalization(timeUnit = CloudWatchTimeUnit.MILLISECONDS, infoUnit = None, rateUnit = None),
      fields = List.empty
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
  unitNormalization: UnitNormalization,
  fields: List[HistogramField]) {
  private def add(hf: HistogramField): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client, storageResolution, unitNormalization, hf :: fields)

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
    new CloudWatchObserver(client, storageResolution, unitNormalization, fields)
  }

  def withTimeUnit(f: CloudWatchTimeUnit.type => NJTimeUnit): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client,
      storageResolution,
      unitNormalization.focus(_.timeUnit).replace(f(CloudWatchTimeUnit)),
      fields)

  def withInfoUnit(f: NJInformationUnit.type => NJInformationUnit): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client,
      storageResolution,
      unitNormalization.focus(_.infoUnit).replace(Some(f(NJInformationUnit))),
      fields)

  def withRateUnit(f: NJDataRateUnit.type => NJDataRateUnit): CloudWatchObserver[F] =
    new CloudWatchObserver[F](
      client,
      storageResolution,
      unitNormalization.focus(_.rateUnit).replace(Some(f(NJDataRateUnit))),
      fields)

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
        id = StableMetricID(timer.metricId),
        category = s"${timer.metricId.tag}_$category",
        standardUnit = toStandardUnit(unitNormalization.timeUnit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, unitNormalization.normalize(dur).value)
    }

    val histograms: List[MetricDatum] = for {
      hf <- interestedHistogramFields
      histo <- report.snapshot.histograms
    } yield {
      val (value, category)      = hf.pick(histo)
      val Normalized(data, unit) = unitNormalization.normalize(histo.histogram.unit, value)
      MetricKey(
        serviceParams = report.serviceParams,
        id = StableMetricID(histo.metricId),
        category = s"${histo.metricId.tag}_$category",
        standardUnit = toStandardUnit(unit),
        storageResolution = storageResolution
      ).metricDatum(report.timestamp.toInstant, data)
    }

    val timer_calls: Map[MetricKey, Long] = report.snapshot.timers.map { timer =>
      MetricKey(
        serviceParams = report.serviceParams,
        id = StableMetricID(timer.metricId),
        category = s"${timer.metricId.tag}_calls",
        standardUnit = StandardUnit.COUNT,
        storageResolution = storageResolution
      ) -> timer.timer.calls
    }.toMap

    val meter_sum: Map[MetricKey, Long] = report.snapshot.meters.map { meter =>
      val Normalized(data, unit) = unitNormalization.normalize(meter.meter.unit, meter.meter.sum)
      MetricKey(
        serviceParams = report.serviceParams,
        id = StableMetricID(meter.metricId),
        category = s"${meter.metricId.tag}_sum",
        standardUnit = toStandardUnit(unit),
        storageResolution = storageResolution
      ) -> data.toLong
    }.toMap

    val histogram_updates: Map[MetricKey, Long] = report.snapshot.histograms.map { histo =>
      MetricKey(
        serviceParams = report.serviceParams,
        id = StableMetricID(histo.metricId),
        category = s"${histo.metricId.tag}_updates",
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
}

final private case class StableMetricID(
  metricName: MetricName,
  tag: String
)

private object StableMetricID {
  def apply(id: MetricID): StableMetricID =
    id.transformInto[StableMetricID]
}

final private case class MetricKey(
  serviceParams: ServiceParams,
  id: StableMetricID,
  category: String,
  standardUnit: StandardUnit,
  storageResolution: Int) {
  def metricDatum(ts: Instant, value: Double): MetricDatum =
    MetricDatum
      .builder()
      .dimensions(
        Dimension.builder().name(CONSTANT_TASK).value(serviceParams.taskName.value).build(),
        Dimension.builder().name(CONSTANT_SERVICE_ID).value(serviceParams.serviceId.show).build(),
        Dimension.builder().name(CONSTANT_HOST).value(serviceParams.hostName.value).build(),
        Dimension
          .builder()
          .name(metricConstants.METRICS_LAUNCH_TIME)
          .value(serviceParams.zerothTick.zonedLaunchTime.toLocalDate.show)
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
