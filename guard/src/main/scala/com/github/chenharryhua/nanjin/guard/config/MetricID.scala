package com.github.chenharryhua.nanjin.guard.config

import cats.{Order, Show}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import org.apache.commons.codec.digest.DigestUtils
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

sealed abstract class CounterKind(override val entryName: String)
    extends EnumEntry with Product with Serializable
object CounterKind extends Enum[CounterKind] with CirceEnum[CounterKind] with CatsEnum[CounterKind] {
  val values: IndexedSeq[CounterKind] = findValues

  case object Dropwizard extends CounterKind("count")

  case object ActionDone extends CounterKind("action_done")
  case object ActionFail extends CounterKind("action_fail")
  case object ActionRetry extends CounterKind("action_retries")

  case object AlertError extends CounterKind("alert_error")
  case object AlertWarn extends CounterKind("alert_warn")
  case object AlertInfo extends CounterKind("alert_info")

  case object HistoCounter extends CounterKind("histogram_count")
  case object MeterCounter extends CounterKind("meter_count")
  case object UdpCounter extends CounterKind("udp_count")
  case object RiskCounter extends CounterKind("risk_count")
}

sealed abstract class TimerKind(override val entryName: String)
    extends EnumEntry with Product with Serializable
object TimerKind extends Enum[TimerKind] with CirceEnum[TimerKind] with CatsEnum[TimerKind] {
  val values: IndexedSeq[TimerKind] = findValues

  case object Dropwizard extends TimerKind("timer")
  case object ActionDoneTimer extends TimerKind("action_done_timer")
  case object ActionFailTimer extends TimerKind("action_fail_timer")
}

sealed abstract class HistogramKind(override val entryName: String)
    extends EnumEntry with Product with Serializable
object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] with CatsEnum[HistogramKind] {
  val values: IndexedSeq[HistogramKind] = findValues

  case object Dropwizard extends HistogramKind("histogram")
  case object UdpHistogram extends HistogramKind("udp_histogram")
}

sealed abstract class GaugeKind(override val entryName: String)
    extends EnumEntry with Product with Serializable
object GaugeKind extends Enum[GaugeKind] with CirceEnum[GaugeKind] with CatsEnum[GaugeKind] {
  val values: IndexedSeq[GaugeKind] = findValues

  case object Dropwizard extends GaugeKind("gauge")
  case object TimedGauge extends GaugeKind("timed_gauge")
  case object RefGauge extends GaugeKind("ref_gauge")
}

sealed abstract class MeterKind(override val entryName: String)
    extends EnumEntry with Product with Serializable
object MeterKind extends Enum[MeterKind] with CirceEnum[MeterKind] with CatsEnum[MeterKind] {
  val values: IndexedSeq[MeterKind] = findValues

  case object Dropwizard extends MeterKind("meter")
}

@JsonCodec
sealed abstract class Category(val name: String) extends Product with Serializable
object Category {
  final case class Gauge(kind: GaugeKind) extends Category(kind.entryName)
  final case class Timer(kind: TimerKind) extends Category(kind.entryName)
  final case class Counter(kind: CounterKind) extends Category(kind.entryName)
  final case class Meter(kind: MeterKind, unit: StandardUnit) extends Category(kind.entryName)
  final case class Histogram(kind: HistogramKind, unit: StandardUnit) extends Category(kind.entryName)
}

@JsonCodec
final case class MetricName(value: String, digest: String, measurement: String)
object MetricName {
  implicit val showMetricName: Show[MetricName] = cats.derived.semiauto.show
  implicit val orderingMetricName: Ordering[MetricName] =
    (x: MetricName, y: MetricName) => x.value.compare(y.value)
  implicit val orderMetricName: Order[MetricName] = Order.fromOrdering

  def apply(serviceParams: ServiceParams, measurement: Measurement, name: String): MetricName = {
    val withPrefix = serviceParams.metricParams.namePrefix + name
    val fullName: List[String] =
      serviceParams.taskParams.taskName :: serviceParams.serviceName :: measurement.value :: withPrefix :: Nil
    val digest = DigestUtils.sha256Hex(fullName.mkString("/")).take(8)

    MetricName(
      value = withPrefix,
      digest = digest,
      measurement = measurement.value
    )
  }
}

@JsonCodec
final case class MetricID(metricName: MetricName, category: Category)
object MetricID {
  implicit val showMetricID: Show[MetricID] = cats.derived.semiauto.show

  def apply(
    serviceParams: ServiceParams,
    measurement: Measurement,
    category: Category,
    name: String): MetricID =
    MetricID(
      metricName = MetricName(
        serviceParams = serviceParams,
        measurement = measurement,
        name = name
      ),
      category = category)
}
