package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Unique
import cats.implicits.catsSyntaxHash
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit
import enumeratum.values.{IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.Encoder
import io.circe.generic.JsonCodec
import org.apache.commons.codec.digest.DigestUtils

sealed abstract class CategoryGroup(override val value: Int)
    extends IntEnumEntry with Product with Serializable

object CategoryGroup extends IntEnum[CategoryGroup] with IntCirceEnum[CategoryGroup] {
  override val values: IndexedSeq[CategoryGroup] = findValues

  case object HealthCheck extends CategoryGroup(1) // gauge
  case object Deadlocks extends CategoryGroup(2) // gauge
  case object Alert extends CategoryGroup(3) // counter
  case object Risk extends CategoryGroup(4) // counter

  case object Timed extends CategoryGroup(5) // gauge
  case object Ratio extends CategoryGroup(6) // gauge
  case object Instrument extends CategoryGroup(7) // gauge

  case object Gauge extends CategoryGroup(8)
  case object Counter extends CategoryGroup(9)

  case object Meter extends CategoryGroup(10)
  case object Histogram extends CategoryGroup(11)
  case object Timer extends CategoryGroup(12)

  case object FlowMeter extends CategoryGroup(13)
  case object Action extends CategoryGroup(14)
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
    case object Deadlocks extends GaugeKind("deadlocks", CategoryGroup.Deadlocks)
    case object Timed extends GaugeKind("elapsed", CategoryGroup.Timed)
    case object Instrument extends GaugeKind("instrument", CategoryGroup.Instrument)
    case object Ratio extends GaugeKind("ratio", CategoryGroup.Ratio)

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

    case object Meter extends CounterKind("meter_sum", CategoryGroup.Meter)
    case object Histogram extends CounterKind("histogram_updates", CategoryGroup.Histogram)
    case object Timer extends CounterKind("timer_calls", CategoryGroup.Timer)

    case object ActionDone extends CounterKind("action_done", CategoryGroup.Action)
    case object ActionFail extends CounterKind("action_fail", CategoryGroup.Action)
    case object ActionRetry extends CounterKind("action_retry", CategoryGroup.Action)

    case object FlowMeter extends CounterKind("flow_updates", CategoryGroup.FlowMeter)

    case object Counter extends CounterKind("count", CategoryGroup.Counter)
  }

  sealed abstract class MeterKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object MeterKind extends Enum[MeterKind] with CirceEnum[MeterKind] {
    val values: IndexedSeq[MeterKind] = findValues

    case object FlowMeter extends MeterKind("flow_meter", CategoryGroup.FlowMeter)

    case object Meter extends MeterKind("meter", CategoryGroup.Meter)
  }

  sealed abstract class HistogramKind(override val entryName: String, val group: CategoryGroup)
      extends CategoryKind

  object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] {
    val values: IndexedSeq[HistogramKind] = findValues

    case object FlowMeter extends HistogramKind("flow_histogram", CategoryGroup.FlowMeter)

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

@JsonCodec
sealed abstract class Category(val kind: CategoryKind, val order: Int) extends Product with Serializable
object Category {
  import CategoryKind.*
  final case class Gauge(override val kind: GaugeKind) extends Category(kind, 1)
  final case class Counter(override val kind: CounterKind) extends Category(kind, 2)
  final case class Meter(override val kind: MeterKind, unit: MeasurementUnit) extends Category(kind, 3)
  final case class Histogram(override val kind: HistogramKind, unit: MeasurementUnit)
      extends Category(kind, 4)
  final case class Timer(override val kind: TimerKind) extends Category(kind, 5)
}

@JsonCodec
final case class MetricName(name: String, digest: String, measurement: String)
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
final case class MetricID(metricName: MetricName, category: Category, uniqueToken: Int) {
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
}

object MetricID {
  def apply(metricName: MetricName, category: Category, token: Unique.Token): MetricID =
    MetricID(metricName, category, token.hash)
}
