package com.github.chenharryhua.nanjin.guard.config

import cats.{Order, Show}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import org.apache.commons.codec.digest.DigestUtils
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

@JsonCodec
final case class Measurement(value: String) extends AnyVal
@JsonCodec
final case class Digest(value: String) extends AnyVal

@JsonCodec
final case class MetricName private (value: String, digest: Digest, measurement: Measurement) {
  val display: String = s"[${digest.value}][$value]"
}
object MetricName {
  implicit val showMetricName: Show[MetricName] = cats.derived.semiauto.show
  implicit val orderingMetricName: Ordering[MetricName] =
    (x: MetricName, y: MetricName) => x.value.compare(y.value)
  implicit val orderMetricName: Order[MetricName] = Order.fromOrdering

  def apply(serviceParams: ServiceParams, measurement: Measurement, name: String): MetricName = {
    val withPrefix = serviceParams.metricParams.namePrefix + name
    val fullName: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: measurement.value :: withPrefix :: Nil
    val digest = Digest(DigestUtils.sha1Hex(fullName.mkString("/")).take(8))
    MetricName(withPrefix, digest, measurement)
  }
}

sealed abstract class CounterKind(override val entryName: String) extends EnumEntry
object CounterKind extends Enum[CounterKind] with CirceEnum[CounterKind] {
  val values: IndexedSeq[CounterKind] = findValues

  object ActionDone extends CounterKind("action_done")
  object ActionFail extends CounterKind("action_fail")
  object ActionRetry extends CounterKind("action_retries")

  object AlertError extends CounterKind("alert_error")
  object AlertWarn extends CounterKind("alert_warn")
  object AlertInfo extends CounterKind("alert_info")

  object HistoCounter extends CounterKind("histogram_count")
  object MeterCounter extends CounterKind("meter_count")

  object UdpCounter extends CounterKind("udp_count")

  object RiskCounter extends CounterKind("risk_count")
}

sealed abstract class TimerKind(override val entryName: String) extends EnumEntry
object TimerKind extends Enum[TimerKind] with CirceEnum[TimerKind] {
  val values: IndexedSeq[TimerKind] = findValues

  object ActionTimer extends TimerKind("action_timer")
}

sealed abstract class HistogramKind(override val entryName: String) extends EnumEntry
object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] {
  val values: IndexedSeq[HistogramKind] = findValues

  object UdpHistogram extends HistogramKind("udp_histogram")
}

sealed abstract class GaugeKind(override val entryName: String) extends EnumEntry
object GaugeKind extends Enum[GaugeKind] with CirceEnum[GaugeKind] {
  val values: IndexedSeq[GaugeKind] = findValues

  object TimedGauge extends GaugeKind("timed_gauge")
}

@JsonCodec
sealed trait Category { def name: String }

object Category {
  final case class Gauge(sub: Option[GaugeKind]) extends Category {
    override val name: String = sub.fold("gauge")(_.entryName)
  }
  final case class Timer(sub: TimerKind) extends Category {
    override val name: String = sub.entryName
  }
  final case class Meter(unit: StandardUnit) extends Category {
    override val name: String = "meter"
  }
  final case class Histogram(unit: StandardUnit, sub: Option[HistogramKind]) extends Category {
    override val name: String = sub.fold("histogram")(_.entryName)
  }
  final case class Counter(sub: Option[CounterKind]) extends Category {
    override val name: String = sub.fold("count")(_.entryName)
  }
}

@JsonCodec
final case class MetricID(metricName: MetricName, category: Category) {
  val display: String = s"${metricName.display}.${category.name}"
}
object MetricID {

  implicit val showMetricID: Show[MetricID] = cats.derived.semiauto.show

  def apply(
    serviceParams: ServiceParams,
    measurement: Measurement,
    category: Category,
    name: String): MetricID =
    MetricID(MetricName(serviceParams, measurement, name), category)
}
