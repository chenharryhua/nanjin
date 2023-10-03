package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Clock
import cats.implicits.{toFunctorOps, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.chrono.Tick
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.cats.time.instances.{duration, zoneddatetime}

import java.time.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class MetricParams(namePrefix: String, rateTimeUnit: TimeUnit, durationTimeUnit: TimeUnit) {

  val rateUnitName: String = rateTimeUnit.name().toLowerCase.dropRight(1)

  // dropwizard default is times / second
  def rateConversion(rate: Double): Double = rate * rateTimeUnit.toSeconds(1)
}

object MetricParams {
  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@JsonCodec
final case class ServicePolicies(restart: String, metricReport: String, metricReset: String) // for display

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
  metricParams: MetricParams,
  brief: Option[Json],
  serviceId: UUID,
  launchTime: ZonedDateTime
) {
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

  implicit val showServiceParams: Show[ServiceParams] = cats.derived.semiauto.show[ServiceParams]

  def apply(
    serviceName: ServiceName,
    taskParams: TaskParams,
    servicePolicies: ServicePolicies,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zeroth: Tick
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName.value,
      taskParams = taskParams,
      servicePolicies = servicePolicies,
      emberServerParams = emberServerParams,
      threshold = None,
      metricParams = MetricParams(
        namePrefix = "",
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      ),
      brief = brief.value,
      serviceId = zeroth.sequenceId,
      launchTime = zeroth.launchTime.atZone(zeroth.zoneId)
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskParams: TaskParams) extends ServiceConfigF[K]

  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithMetricNamePrefix[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithRestartThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    servicePolicies: ServicePolicies,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zeroth: Tick): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskParams) =>
        ServiceParams(
          serviceName = serviceName,
          taskParams = taskParams,
          servicePolicies = servicePolicies,
          emberServerParams = emberServerParams,
          brief = brief,
          zeroth = zeroth
        )

      case WithRateTimeUnit(v, c)     => c.focus(_.metricParams.rateTimeUnit).replace(v)
      case WithDurationTimeUnit(v, c) => c.focus(_.metricParams.durationTimeUnit).replace(v)
      case WithMetricNamePrefix(v, c) => c.focus(_.metricParams.namePrefix).replace(v)
      case WithRestartThreshold(v, c) => c.focus(_.threshold).replace(v)

    }
}

final case class ServiceConfig(cont: Fix[ServiceConfigF]) extends AnyVal {
  import ServiceConfigF.*

  // metrics
  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithRateTimeUnit(tu, cont)))

  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithDurationTimeUnit(tu, cont)))

  def withMetricNamePrefix(prefix: String): ServiceConfig =
    ServiceConfig(Fix(WithMetricNamePrefix(prefix, cont)))

  def withRestartThreshold(fd: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRestartThreshold(Some(fd.toJava), cont)))

  def evalConfig(
    serviceName: ServiceName,
    servicePolicies: ServicePolicies,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zeroth: Tick): ServiceParams =
    scheme
      .cata(
        algebra(
          serviceName = serviceName,
          servicePolicies = servicePolicies,
          emberServerParams = emberServerParams,
          brief = brief,
          zeroth = zeroth
        ))
      .apply(cont)
}

object ServiceConfig {

  def apply(taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskParams)))
}
