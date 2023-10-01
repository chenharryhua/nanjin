package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.effect.kernel.Resource.ExitCase
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.generic.JsonCodec
import org.apache.commons.lang3.exception.ExceptionUtils

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
}
