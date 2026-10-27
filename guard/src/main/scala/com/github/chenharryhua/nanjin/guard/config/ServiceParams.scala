package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Policy
import io.circe.jawn.parse
import io.circe.{Codec, Encoder, Json}

import java.time.*

final case class ServiceIdentity(
  task: Task,
  service: Service,
  host: Host,
  serviceId: ServiceId,
  launchTime: LaunchTime,
  homepage: Option[Homepage]
) derives Codec.AsObject {
  val timeZone: TimeZone = TimeZone(launchTime.zoneId)
}

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
  serviceIdentity: ServiceIdentity,
  policies: ServicePolicies,
  history: Option[HistoryCapacity],
  logFormat: Option[LogFormat],
  nanjin: Option[Json],
  brief: Brief
) derives Codec.AsObject

object ServiceParams {
  def apply(
    taskName: Task,
    serviceName: Service,
    serviceId: ServiceId,
    launchTime: LaunchTime,
    brief: Brief,
    host: Host
  ): ServiceParams =
    ServiceParams(
      serviceIdentity = ServiceIdentity(
        task = taskName,
        service = serviceName,
        serviceId = serviceId,
        homepage = None,
        host = host,
        launchTime = launchTime
      ),
      policies = ServicePolicies(
        restart = RestartPolicy(Policy.empty, None),
        dashboard = None,
        report = Policy.empty
      ),
      history = None,
      logFormat = None,
      // Best-effort build metadata: `BuildInfo.toJson` is generated and expected to always parse, so a
      // parse failure is swallowed to `None` rather than failing service construction. The trade-off is
      // that malformed build info would silently drop the `nanjin` field from every event with no signal;
      // this is deliberate, so `nanjin` must be treated as optional by every consumer.
      nanjin = parse(BuildInfo.toJson).toOption,
      brief = brief
    )
}
