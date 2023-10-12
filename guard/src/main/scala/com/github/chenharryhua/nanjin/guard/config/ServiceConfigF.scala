package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import cats.effect.kernel.Clock
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, policies, Policy, Tick}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.cats.time.instances.{duration, zoneddatetime}

import java.time.*
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class ServicePolicies(restart: Policy, metricReport: Policy, metricReset: Policy)

@JsonCodec
final case class EmberServerParams(
  host: Option[String],
  port: Int,
  maxConnections: Int,
  receiveBufferSize: Int,
  maxHeaderSize: Int)

object EmberServerParams {
  def apply[F[_]](esb: EmberServerBuilder[F]): EmberServerParams =
    EmberServerParams(
      host = esb.host.map(_.show),
      port = esb.port.value,
      maxConnections = esb.maxConnections,
      receiveBufferSize = esb.receiveBufferSize,
      maxHeaderSize = esb.maxHeaderSize
    )
}

@JsonCodec
final case class ServiceParams(
  serviceName: String,
  servicePolicies: ServicePolicies,
  emberServerParams: Option[EmberServerParams],
  threshold: Option[Duration],
  taskParams: TaskParams,
  brief: Option[Json],
  zerothTick: Tick
) {
  val serviceId: UUID                             = zerothTick.sequenceId
  val launchTime: ZonedDateTime                   = zerothTick.launchTime.atZone(zerothTick.zoneId)
  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(taskParams.zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    toZonedDateTime(Instant.EPOCH.plusNanos(fd.toNanos))

  def toLocalDateTime(ts: Instant): LocalDateTime = toZonedDateTime(ts).toLocalDateTime
  def toLocalDate(ts: Instant): LocalDate         = toZonedDateTime(ts).toLocalDate
  def toLocalTime(ts: Instant): LocalTime         = toZonedDateTime(ts).toLocalTime
  def upTime(ts: ZonedDateTime): Duration         = Duration.between(launchTime, ts)
  def upTime(ts: Instant): Duration               = Duration.between(launchTime, toZonedDateTime(ts))

  def zonedNow[F[_]: Clock: Functor]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
}

object ServiceParams extends zoneddatetime with duration {

  def apply(
    serviceName: ServiceName,
    taskParams: TaskParams,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zerothTick: Tick
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName.value,
      taskParams = taskParams,
      servicePolicies = ServicePolicies(
        restart = policies.giveUp,
        metricReport = policies.giveUp,
        metricReset = policies.giveUp),
      emberServerParams = emberServerParams,
      threshold = None,
      brief = brief.value,
      zerothTick = zerothTick
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskParams: TaskParams) extends ServiceConfigF[K]

  final case class WithRestartThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]
  final case class WithRestartPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithMetricReportPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithMetricResetPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zerothTick: Tick): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskParams) =>
        ServiceParams(
          serviceName = serviceName,
          taskParams = taskParams,
          emberServerParams = emberServerParams,
          brief = brief,
          zerothTick = zerothTick
        )

      case WithRestartThreshold(v, c)   => c.focus(_.threshold).replace(v)
      case WithRestartPolicy(v, c)      => c.focus(_.servicePolicies.restart).replace(v)
      case WithMetricReportPolicy(v, c) => c.focus(_.servicePolicies.metricReport).replace(v)
      case WithMetricResetPolicy(v, c)  => c.focus(_.servicePolicies.metricReset).replace(v)
    }
}

final case class ServiceConfig(cont: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withRestartThreshold(fd: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRestartThreshold(Some(fd.toJava), cont)))

  def withRestartPolicy(restart: Policy): ServiceConfig =
    ServiceConfig(Fix(WithRestartPolicy(restart, cont)))

  def withMetricReport(report: Policy): ServiceConfig =
    ServiceConfig(Fix(WithMetricReportPolicy(report, cont)))

  def withMetricReset(reset: Policy): ServiceConfig =
    ServiceConfig(Fix(WithMetricResetPolicy(reset, cont)))

  def withMetricDailyReset: ServiceConfig =
    withMetricReset(policies.crontab(crontabs.daily.midnight))

  def evalConfig(
    serviceName: ServiceName,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zerothTick: Tick): ServiceParams =
    scheme
      .cata(
        algebra(
          serviceName = serviceName,
          emberServerParams = emberServerParams,
          brief = brief,
          zerothTick = zerothTick
        ))
      .apply(cont)
}

object ServiceConfig {

  def apply(taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskParams)))
}
