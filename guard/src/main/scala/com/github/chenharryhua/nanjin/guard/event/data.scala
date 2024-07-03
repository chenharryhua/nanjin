package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.effect.kernel.Unique
import cats.implicits.{catsSyntaxHash, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters.ListHasAsScala

@JsonCodec
final case class NJError(message: String, stack: List[String])

object NJError {
  def apply(ex: Throwable): NJError =
    NJError(
      ExceptionUtils.getRootCauseMessage(ex),
      ExceptionUtils.getRootCauseStackTraceList(ex).asScala.toList.map(_.replace("\t", "")))
}

@JsonCodec
sealed trait MetricIndex extends Product with Serializable {
  def launchTime: ZonedDateTime
}

object MetricIndex {
  final case class Adhoc(value: ZonedDateTime) extends MetricIndex {
    override val launchTime: ZonedDateTime = value
  }
  final case class Periodic(tick: Tick) extends MetricIndex {
    override val launchTime: ZonedDateTime = tick.zonedWakeup
  }
}

sealed abstract class ServiceStopCause(val exitCode: Int) extends Product with Serializable

object ServiceStopCause {
  case object Normally extends ServiceStopCause(0)
  case object Maintenance extends ServiceStopCause(1)
  case object ByCancellation extends ServiceStopCause(2)
  final case class ByException(error: NJError) extends ServiceStopCause(3)

  private val NORMALLY: String        = "Normally"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String     = "Maintenance"
  private val BY_EXCEPTION: String    = "ByException"

  implicit val encoderServiceStopCause: Encoder[ServiceStopCause] = Encoder.instance {
    case Normally           => Json.fromString(NORMALLY)
    case ByCancellation     => Json.fromString(BY_CANCELLATION)
    case ByException(error) => Json.obj(BY_EXCEPTION -> error.asJson)
    case Maintenance        => Json.fromString(MAINTENANCE)
  }

  implicit val decoderServiceStopCause: Decoder[ServiceStopCause] =
    List[Decoder[ServiceStopCause]](
      _.as[String].flatMap {
        case NORMALLY        => Right(Normally)
        case BY_CANCELLATION => Right(ByCancellation)
        case MAINTENANCE     => Right(Maintenance)
        case unknown         => Left(DecodingFailure(s"unrecognized: $unknown", Nil))
      }.widen,
      _.downField(BY_EXCEPTION).as[NJError].map(err => ByException(err)).widen
    ).reduceLeft(_ or _)
}

final case class ActionID(uniqueToken: Int) extends AnyVal
object ActionID {
  implicit val showActionID: Show[ActionID]       = _.uniqueToken.toString
  implicit val encoderActionID: Encoder[ActionID] = Encoder.encodeInt.contramap(_.uniqueToken)
  implicit val decoderActionID: Decoder[ActionID] = Decoder.decodeInt.map(ActionID(_))

  def apply(token: Unique.Token): ActionID = ActionID(token.hash)
}
