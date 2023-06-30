package com.github.chenharryhua.nanjin.guard.event

import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import cats.{Monad, Order, Show}
import io.circe.Codec
import io.circe.generic.JsonCodec
import natchez.Span
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.{duration, instant}
import retry.RetryStatus

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class NJError private (message: String, stackTrace: String)

private[guard] object NJError {
  implicit final val showNJError: Show[NJError] = cats.derived.semiauto.show[NJError]
  def apply(ex: Throwable): NJError =
    NJError(ExceptionUtils.getRootCauseMessage(ex), ExceptionUtils.getStackTrace(ex))
}

@JsonCodec
sealed trait MetricIndex extends Product with Serializable

object MetricIndex {
  case object Adhoc extends MetricIndex
  final case class Periodic(index: Int) extends MetricIndex
}

@JsonCodec
final case class ActionInfo(actionId: Int, launchTime: FiniteDuration, traceInfo: Option[TraceInfo]) {
  def took(landTime: FiniteDuration): Duration = landTime.minus(launchTime).toJava
  def traceId: Option[String]                  = traceInfo.map(_.traceId)
}

object ActionInfo {
  implicit final val showActionInfo: Show[ActionInfo] = cats.derived.semiauto.show[ActionInfo]

  implicit final val tokenCodec: Codec.AsObject[Either[Int, TraceInfo]] =
    Codec.codecForEither[Int, TraceInfo]("hash", "trace")
}

@JsonCodec
sealed abstract class ServiceStopCause(val exitCode: Int) extends Product with Serializable {

  final override def toString: String = this match {
    case ServiceStopCause.Normally         => "normally exit"
    case ServiceStopCause.ByCancelation    => "abnormally exit due to cancelation"
    case ServiceStopCause.ByException(msg) => s"abnormally exit due to $msg"
    case ServiceStopCause.ByGiveup(msg)    => s"give up due to $msg"
  }
}

object ServiceStopCause {
  def apply(ec: ExitCase): ServiceStopCause = ec match {
    case ExitCase.Succeeded  => ServiceStopCause.Normally
    case ExitCase.Errored(e) => ServiceStopCause.ByException(ExceptionUtils.getRootCauseMessage(e))
    case ExitCase.Canceled   => ServiceStopCause.ByCancelation
  }

  implicit final val showServiceStopCause: Show[ServiceStopCause] = Show.fromToString

  case object Normally extends ServiceStopCause(0)
  case object ByCancelation extends ServiceStopCause(1)
  final case class ByException(msg: String) extends ServiceStopCause(2)
  final case class ByGiveup(msg: String) extends ServiceStopCause(3)
}

@JsonCodec
final case class TraceInfo(traceId: String, spanId: String)
object TraceInfo {
  implicit final val showTraceInfo: Show[TraceInfo] = cats.derived.semiauto.show

  def apply[F[_]: Monad](span: Span[F]): F[Option[TraceInfo]] = for {
    tid <- span.traceId
    sid <- span.spanId
  } yield (tid, sid).mapN(TraceInfo(_, _))
}

/** @param pullTime
  *   when it is pulled
  * @param wakeTime
  *   when sleep is over
  * @param delay
  *   nominal delay, actual delay is equal to wakeTime minus pullTime roughly
  */
@JsonCodec
final case class Tick(index: Int, pullTime: Instant, wakeTime: Instant, delay: Duration)

object Tick extends duration with instant {
  implicit val orderingTick: Ordering[Tick] = Ordering.by(_.index)
  implicit val orderTick: Order[Tick]       = Order.fromOrdering[Tick]
  implicit val showTick: Show[Tick]         = cats.derived.semiauto.show[Tick]

  final val Zero: Tick = Tick(
    index = RetryStatus.NoRetriesYet.retriesSoFar,
    pullTime = Instant.ofEpochMilli(0),
    wakeTime = Instant.ofEpochMilli(0),
    delay = Duration.ZERO
  )
}
