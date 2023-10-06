package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.effect.kernel.Resource.ExitCase
import com.github.chenharryhua.nanjin.common.chrono.Tick
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, EnumEntry}
import io.circe.generic.JsonCodec
import org.apache.commons.lang3.exception.ExceptionUtils

import java.util.concurrent.TimeUnit

@JsonCodec
final case class NJError(message: String, stackTrace: String)

object NJError {
  implicit final val showNJError: Show[NJError] = cats.derived.semiauto.show[NJError]
  def apply(ex: Throwable): NJError =
    NJError(ExceptionUtils.getRootCauseMessage(ex), ExceptionUtils.getStackTrace(ex))
}

@JsonCodec
sealed trait MetricIndex extends Product with Serializable

object MetricIndex {
  case object Adhoc extends MetricIndex
  final case class Periodic(tick: Tick) extends MetricIndex
}

@JsonCodec
sealed abstract class ServiceStopCause(val exitCode: Int) extends Product with Serializable {

  final override def toString: String = this match {
    case ServiceStopCause.Normally         => "normally exit"
    case ServiceStopCause.ByCancellation   => "abnormally exit due to cancellation"
    case ServiceStopCause.ByException(msg) => s"abnormally exit due to $msg"
    case ServiceStopCause.ByUser           => "stop by user"
  }
}

object ServiceStopCause {
  def apply(ec: ExitCase): ServiceStopCause = ec match {
    case ExitCase.Succeeded  => ServiceStopCause.Normally
    case ExitCase.Errored(e) => ServiceStopCause.ByException(ExceptionUtils.getRootCauseMessage(e))
    case ExitCase.Canceled   => ServiceStopCause.ByCancellation
  }

  implicit final val showServiceStopCause: Show[ServiceStopCause] = Show.fromToString

  case object Normally extends ServiceStopCause(0)
  case object ByCancellation extends ServiceStopCause(1)
  final case class ByException(msg: String) extends ServiceStopCause(2)
  case object ByUser extends ServiceStopCause(3)
}

// consistent with software.amazon.awssdk.services.cloudwatch.model.StandardUnit
sealed trait MeasurementUnit extends EnumEntry

sealed abstract class DurationUnit(val timeUnit: TimeUnit) extends MeasurementUnit

object DurationUnit
    extends enumeratum.Enum[DurationUnit] with CirceEnum[DurationUnit] with CatsEnum[DurationUnit]
    with Lowercase {
  val values: IndexedSeq[DurationUnit] = findValues
  case object DAYS extends DurationUnit(TimeUnit.DAYS)
  case object HOURS extends DurationUnit(TimeUnit.HOURS)
  case object MINUTES extends DurationUnit(TimeUnit.MINUTES)
  case object SECONDS extends DurationUnit(TimeUnit.SECONDS)
  case object MILLISECONDS extends DurationUnit(TimeUnit.MILLISECONDS)
  case object MICROSECONDS extends DurationUnit(TimeUnit.MICROSECONDS)
  case object NANOSECONDS extends DurationUnit(TimeUnit.NANOSECONDS)
}

object MeasurementUnit
    extends enumeratum.Enum[MeasurementUnit] with CirceEnum[MeasurementUnit] with CatsEnum[MeasurementUnit]
    with Lowercase {
  val values: IndexedSeq[MeasurementUnit] = findValues

  case object BYTES extends MeasurementUnit
  case object KILOBYTES extends MeasurementUnit
  case object MEGABYTES extends MeasurementUnit
  case object GIGABYTES extends MeasurementUnit
  case object TERABYTES extends MeasurementUnit
  case object BITS extends MeasurementUnit
  case object KILOBITS extends MeasurementUnit
  case object MEGABITS extends MeasurementUnit
  case object GIGABITS extends MeasurementUnit
  case object TERABITS extends MeasurementUnit
  case object PERCENT extends MeasurementUnit
  case object COUNT extends MeasurementUnit
  case object BYTES_SECOND extends MeasurementUnit
  case object KILOBYTES_SECOND extends MeasurementUnit
  case object MEGABYTES_SECOND extends MeasurementUnit
  case object GIGABYTES_SECOND extends MeasurementUnit
  case object TERABYTES_SECOND extends MeasurementUnit
  case object BITS_SECOND extends MeasurementUnit
  case object KILOBITS_SECOND extends MeasurementUnit
  case object MEGABITS_SECOND extends MeasurementUnit
  case object GIGABITS_SECOND extends MeasurementUnit
  case object TERABITS_SECOND extends MeasurementUnit
  case object COUNT_SECOND extends MeasurementUnit
}
