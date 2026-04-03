package com.github.chenharryhua.nanjin.guard.config
import cats.derived.derived
import cats.effect.kernel.Clock
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.functor.given
import cats.{Applicative, Endo, Functor, Show}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.{TimeZone, UpTime}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.jawn.parse
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final case class RestartPolicy(policy: Policy, capacity: Capacity, threshold: Option[Duration])
    derives Codec.AsObject
final case class DashboardPolicy(policy: Policy, maxPoints: Capacity) derives Codec.AsObject
final case class MetricsReportPolicy(policy: Policy, capacity: Capacity) derives Codec.AsObject

final case class ServicePolicies(
  restart: RestartPolicy,
  dashboard: Option[DashboardPolicy],
  report: MetricsReportPolicy,
  metricsReset: Policy
) derives Codec.AsObject

final case class HistoryCapacity(error: Capacity) derives Codec.AsObject

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
  servicePolicies: ServicePolicies,
  historyCapacity: HistoryCapacity,
  logFormat: LogFormat,
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
  private val defaultCapacity: Capacity = Capacity(32)
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
      servicePolicies = ServicePolicies(
        restart = RestartPolicy(Policy.empty, defaultCapacity, None),
        dashboard = None,
        report = MetricsReportPolicy(Policy.empty, defaultCapacity),
        metricsReset = Policy.empty
      ),
      historyCapacity = HistoryCapacity(defaultCapacity),
      logFormat = LogFormat.Console_PlainText,
      nanjin = parse(BuildInfo.toJson).toOption,
      brief = brief
    )
}

sealed private[guard] trait ServiceConfigF[X] extends Product derives Functor

private object ServiceConfigF {

  final case class InitParams[K](taskName: Task) extends ServiceConfigF[K]

  final case class WithMetricReportPolicy[K](policy: Policy, history: Option[Int], cont: K)
      extends ServiceConfigF[K]
  final case class WithRestartPolicy[K](policy: Policy, history: Option[Int], threshold: Duration, cont: K)
      extends ServiceConfigF[K]
  final case class WithMetricResetPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]

  final case class WithHomePage[K](value: Option[Homepage], cont: K) extends ServiceConfigF[K]

  final case class WithErrorCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithTaskName[K](value: Task, cont: K) extends ServiceConfigF[K]

  final case class WithLogFormat[K](value: LogFormat, cont: K) extends ServiceConfigF[K]
  final case class WithDashboardPolicy[K](policy: Policy, maxPoints: Int, cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: Service,
    brief: Brief,
    launchTime: ZonedDateTime,
    serviceId: ServiceId,
    host: Host): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskName) =>
        ServiceParams(
          taskName = taskName,
          serviceName = serviceName,
          brief = brief,
          launchTime = launchTime,
          serviceId = serviceId,
          host = host
        )

      case WithRestartPolicy(p, h, t, c) =>
        c.focus(_.servicePolicies.restart)
          .modify(rp =>
            h.fold(rp.copy(policy = p, threshold = Some(t)))(c => RestartPolicy(p, Capacity(c), Some(t))))
      case WithMetricResetPolicy(v, c) =>
        c.focus(_.servicePolicies.metricsReset).replace(v)
      case WithMetricReportPolicy(p, h, c) =>
        c.focus(_.servicePolicies.report)
          .modify(rp => h.fold(rp.copy(policy = p))(c => MetricsReportPolicy(p, Capacity(c))))
      case WithHomePage(v, c) => c.focus(_.homepage).replace(v)

      case WithErrorCapacity(v, c) => c.focus(_.historyCapacity.error).replace(Capacity(v))

      case WithTaskName(v, c) => c.focus(_.taskName).replace(v)

      case WithLogFormat(v, c) => c.focus(_.logFormat).replace(v)

      case WithDashboardPolicy(p, m, c) =>
        c.focus(_.servicePolicies.dashboard).replace(Some(DashboardPolicy(p, Capacity(m))))
    }
}

final class ServiceConfig[F[_]: Applicative] private (
  cont: Fix[ServiceConfigF],
  private[guard] val zoneId: ZoneId,
  private[guard] val httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  private[guard] val briefs: F[List[Json]],
  private[guard] val alarmLevel: AlarmLevel) {
  import ServiceConfigF.*

  private def copy(
    cont: Fix[ServiceConfigF] = this.cont,
    zoneId: ZoneId = this.zoneId,
    httpBuilder: Option[Endo[EmberServerBuilder[F]]] = this.httpBuilder,
    briefs: F[List[Json]] = this.briefs,
    alarmLevel: AlarmLevel = this.alarmLevel): ServiceConfig[F] =
    new ServiceConfig[F](cont, zoneId, httpBuilder, briefs, alarmLevel)

  def withRestartPolicy(
    threshold: FiniteDuration,
    historyCapacity: Int,
    f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(f(Policy), Some(historyCapacity), threshold.toJava, cont)))
  def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(f(Policy), None, threshold.toJava, cont)))

  def withMetricReport(historyCapacity: Int, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(f(Policy), Some(historyCapacity), cont)))
  def withMetricReport(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(f(Policy), None, cont)))

  def withMetricReset(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricResetPolicy(f(Policy), cont)))

  def withMetricDailyReset: ServiceConfig[F] =
    withMetricReset(_.crontab(_.daily.midnight))

  def withHomePage(hp: String): ServiceConfig[F] =
    copy(cont = Fix(WithHomePage(Some(Homepage(hp)), cont)))

  def withTaskName(tn: String): ServiceConfig[F] =
    copy(cont = Fix(WithTaskName(Task(tn), cont)))

  def withZoneId(zoneId: ZoneId): ServiceConfig[F] =
    copy(zoneId = zoneId)

  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F] =
    copy(httpBuilder = Some(f))
  def disableHttpServer: ServiceConfig[F] =
    copy(httpBuilder = None)

  def withErrorHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithErrorCapacity(value, cont)))

  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F] = copy(briefs = (fa, briefs).mapN(_.asJson :: _))
  def addBrief[A: Encoder](a: => A): ServiceConfig[F] = addBrief(a.pure[F])

  def withLogFormat(f: LogFormat.type => LogFormat): ServiceConfig[F] =
    copy(cont = Fix(WithLogFormat(f(LogFormat), cont)))

  def withInitialAlarmLevel(f: AlarmLevel.type => AlarmLevel): ServiceConfig[F] =
    copy(alarmLevel = f(AlarmLevel))

  def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithDashboardPolicy(f(Policy), maxPoints, cont)))

  private[guard] def evalConfig(
    serviceName: Service,
    serviceId: ServiceId,
    launchTime: ZonedDateTime,
    brief: Brief,
    host: Host): ServiceParams =
    scheme
      .cata(
        algebra(
          serviceName = serviceName,
          serviceId = serviceId,
          launchTime = launchTime,
          brief = brief,
          host = host
        ))
      .apply(cont)
}

private[guard] object ServiceConfig {

  def apply[F[_]: Applicative](taskName: Task): ServiceConfig[F] =
    new ServiceConfig[F](
      cont = Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskName)),
      zoneId = ZoneId.systemDefault(),
      httpBuilder = None,
      briefs = List.empty[Json].pure[F],
      alarmLevel = AlarmLevel.Info
    )
}
