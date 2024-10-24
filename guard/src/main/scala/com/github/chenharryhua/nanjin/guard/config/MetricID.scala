package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Unique
import cats.implicits.catsSyntaxHash
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, UniqueToken}
import enumeratum.values.{IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import io.circe.{Decoder, Encoder}
import org.apache.commons.codec.digest.DigestUtils

sealed abstract class CategoryGroup(override val value: Int)
    extends IntEnumEntry with Product with Serializable
// display order
object CategoryGroup extends IntEnum[CategoryGroup] with IntCirceEnum[CategoryGroup] {
  override val values: IndexedSeq[CategoryGroup] = findValues

  case object HealthCheck extends CategoryGroup(1) // gauge
  case object Alert extends CategoryGroup(2) // counter
  case object Risk extends CategoryGroup(3) // counter

  case object Gauge extends CategoryGroup(4)
  case object Counter extends CategoryGroup(5)

  case object Meter extends CategoryGroup(6)
  case object Histogram extends CategoryGroup(7)
  case object Timer extends CategoryGroup(8)

  case object Action extends CategoryGroup(9) // timer
}

sealed trait CategoryKind extends EnumEntry with Product with Serializable {
  def entryName: String
  def group: CategoryGroup
}

object CategoryKind {
  sealed abstract class GaugeKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object GaugeKind extends Enum[GaugeKind] with CirceEnum[GaugeKind] {
    val values: IndexedSeq[GaugeKind] = findValues

    case object HealthCheck extends GaugeKind("healthy", CategoryGroup.HealthCheck)

    case object Gauge extends GaugeKind("gauge", CategoryGroup.Gauge)
  }

  sealed abstract class CounterKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object CounterKind extends Enum[CounterKind] with CirceEnum[CounterKind] {
    val values: IndexedSeq[CounterKind] = findValues

    case object Risk extends CounterKind("count_risk", CategoryGroup.Risk)

    case object AlertError extends CounterKind("alert_error", CategoryGroup.Alert)
    case object AlertWarn extends CounterKind("alert_warn", CategoryGroup.Alert)
    case object AlertInfo extends CounterKind("alert_info", CategoryGroup.Alert)

    case object ActionDone extends CounterKind("action_done", CategoryGroup.Action)
    case object ActionFail extends CounterKind("action_fail", CategoryGroup.Action)
    case object ActionRetry extends CounterKind("action_retry", CategoryGroup.Action)

    case object Counter extends CounterKind("count", CategoryGroup.Counter)
  }

  sealed abstract class MeterKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object MeterKind extends Enum[MeterKind] with CirceEnum[MeterKind] {
    val values: IndexedSeq[MeterKind] = findValues

    case object Meter extends MeterKind("meter", CategoryGroup.Meter)
  }

  sealed abstract class HistogramKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] {
    val values: IndexedSeq[HistogramKind] = findValues

    case object Histogram extends HistogramKind("histogram", CategoryGroup.Histogram)
  }

  sealed abstract class TimerKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object TimerKind extends Enum[TimerKind] with CirceEnum[TimerKind] {
    val values: IndexedSeq[TimerKind] = findValues

    case object Action extends TimerKind("action_timer", CategoryGroup.Action)

    case object Timer extends TimerKind("timer", CategoryGroup.Timer)
  }
}

final case class MetricTag(value: String) extends AnyVal
object MetricTag {
  implicit val encoderMetricTag: Encoder[MetricTag] = Encoder.encodeString.contramap(_.value)
  implicit val decoderMetricTag: Decoder[MetricTag] = Decoder.decodeString.map(MetricTag(_))
}

@JsonCodec
sealed abstract class Category(val kind: CategoryKind, val order: Int, val tag: MetricTag)
    extends Product with Serializable
object Category {
  import CategoryKind.*

  final case class Gauge(override val kind: GaugeKind, override val tag: MetricTag)
      extends Category(kind, 1, tag)

  final case class Counter(override val kind: CounterKind, override val tag: MetricTag)
      extends Category(kind, 2, tag)

  final case class Meter(override val kind: MeterKind, override val tag: MetricTag, unit: MeasurementUnit)
      extends Category(kind, 3, tag)

  final case class Histogram(
    override val kind: HistogramKind,
    override val tag: MetricTag,
    unit: MeasurementUnit)
      extends Category(kind, 4, tag)

  final case class Timer(override val kind: TimerKind, override val tag: MetricTag)
      extends Category(kind, 5, tag)
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

/** @param uniqueToken
  *   hash of Unique.Token. it is runtime identifier of a metric
  */
@JsonCodec
final case class MetricID private (metricName: MetricName, category: Category, uniqueToken: Int) {
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val tag: String        = category.tag.value
}

object MetricID {
  def apply(metricName: MetricName, category: Category, token: Unique.Token): MetricID =
    MetricID(metricName, category, token.hash)

  def apply(metricName: MetricName, category: Category, token: UniqueToken): MetricID =
    MetricID(metricName, category, token.uniqueToken)
}
