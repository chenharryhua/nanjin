package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Clock
import cats.syntax.functor.given
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.{TimeZone, UpTime}
import io.circe.jawn.parse
import io.circe.{Codec, Encoder, Json}

import java.time.*
import scala.concurrent.duration.FiniteDuration

final case class RestartPolicy(policy: Policy, threshold: Option[Duration]) derives Codec.AsObject
final case class DashboardPolicy(policy: Policy, maxPoints: Capacity) derives Codec.AsObject
final case class HistoryCapacity(panics: Capacity, errors: Capacity, metrics: Capacity) derives Codec.AsObject

final case class ServicePolicies(
  restart: RestartPolicy,
  dashboard: Option[DashboardPolicy],
  report: Policy
) derives Codec.AsObject

final case class Host(name: HostName, port: Option[Port]) derives Codec.AsObject {
  override def toString: String =
    port match {
      case Some(p) => s"${name.value}:${p.value}"
      case None    => name.value
    }
}
object Host {
  given Show[Host] = Show.fromToString[Host]
}

final case class ServiceParams(
  taskName: Task,
  host: Host,
  homepage: Option[Homepage],
  serviceName: Service,
  serviceId: ServiceId,
  launchTime: ZonedDateTime,
  policies: ServicePolicies,
  history: Option[HistoryCapacity],
  logFormat: Option[LogFormat],
  nanjin: Option[Json],
  brief: Brief
) derives Codec.AsObject {
  val zoneId: ZoneId = launchTime.getZone
  val timeZone: TimeZone = TimeZone(zoneId)

  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    Instant.EPOCH.plusNanos(fd.toNanos).atZone(zoneId)

  def upTime(ts: ZonedDateTime): UpTime = UpTime(Duration.between(launchTime, ts))
  def upTime(ts: Instant): UpTime = UpTime(Duration.between(launchTime.toInstant, ts))

  def zonedNow[F[_]: {Clock, Functor}]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
}

object ServiceParams {
  def apply(
    taskName: Task,
    serviceName: Service,
    serviceId: ServiceId,
    launchTime: ZonedDateTime,
    brief: Brief,
    host: Host
  ): ServiceParams =
    ServiceParams(
      taskName = taskName,
      host = host,
      homepage = None,
      serviceName = serviceName,
      serviceId = serviceId,
      launchTime = launchTime,
      policies = ServicePolicies(
        restart = RestartPolicy(Policy.empty, None),
        dashboard = None,
        report = Policy.empty
      ),
      history = None,
      logFormat = None,
      nanjin = parse(BuildInfo.toJson).toOption,
      brief = brief
    )
}
