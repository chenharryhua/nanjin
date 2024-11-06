package com.github.chenharryhua.nanjin.guard.config

import cats.kernel.Order
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.Encoder
import io.circe.generic.JsonCodec
import org.apache.commons.codec.digest.DigestUtils

import scala.concurrent.duration.FiniteDuration

sealed trait CategoryKind extends EnumEntry with Product with Serializable

object CategoryKind {
  sealed trait GaugeKind extends CategoryKind

  object GaugeKind extends Enum[GaugeKind] with CirceEnum[GaugeKind] {
    val values: IndexedSeq[GaugeKind] = findValues

    case object HealthCheck extends GaugeKind
    case object Ratio extends GaugeKind

    case object Gauge extends GaugeKind
  }

  sealed trait CounterKind extends CategoryKind

  object CounterKind extends Enum[CounterKind] with CirceEnum[CounterKind] {
    val values: IndexedSeq[CounterKind] = findValues

    case object Risk extends CounterKind

    case object Counter extends CounterKind
  }

  sealed trait MeterKind extends CategoryKind

  object MeterKind extends Enum[MeterKind] with CirceEnum[MeterKind] {
    val values: IndexedSeq[MeterKind] = findValues

    case object Meter extends MeterKind
  }

  sealed trait HistogramKind extends CategoryKind

  object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] {
    val values: IndexedSeq[HistogramKind] = findValues

    case object Histogram extends HistogramKind
  }

  sealed trait TimerKind extends CategoryKind

  object TimerKind extends Enum[TimerKind] with CirceEnum[TimerKind] {
    val values: IndexedSeq[TimerKind] = findValues

    case object Timer extends TimerKind
  }
}

@JsonCodec
sealed abstract class Category(val kind: CategoryKind) extends Product with Serializable
object Category {
  import CategoryKind.*

  final case class Gauge(override val kind: GaugeKind) extends Category(kind)
  final case class Counter(override val kind: CounterKind) extends Category(kind)
  final case class Meter(override val kind: MeterKind, unit: MeasurementUnit) extends Category(kind)
  final case class Histogram(override val kind: HistogramKind, unit: MeasurementUnit) extends Category(kind)
  final case class Timer(override val kind: TimerKind) extends Category(kind)
}

@JsonCodec
final case class MetricTag private (value: String, order: Long)
object MetricTag {
  implicit val orderingMetricTag: Ordering[MetricTag] = Ordering.by(_.order)
  implicit val orderMetricTag: Order[MetricTag]       = Order.fromOrdering

  def apply(name: String, fd: FiniteDuration): MetricTag =
    MetricTag(name, fd.toNanos)

}

@JsonCodec
final case class MetricName private (name: String, digest: String, measurement: String)
object MetricName {
  def apply(serviceParams: ServiceParams, measurement: Measurement, name: String): MetricName = {
    val full_name: List[String] =
      serviceParams.taskName.value :: serviceParams.serviceName.value :: measurement.value :: name :: Nil
    val digest = DigestUtils.sha256Hex(full_name.mkString("/")).take(8)

    MetricName(
      name = name,
      digest = digest,
      measurement = measurement.value
    )
  }
}

@JsonCodec
final case class MetricID(metricName: MetricName, metricTag: MetricTag, category: Category) {
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val tag: String        = metricTag.value
}
