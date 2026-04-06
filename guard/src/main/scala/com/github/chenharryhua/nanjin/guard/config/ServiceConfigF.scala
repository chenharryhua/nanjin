package com.github.chenharryhua.nanjin.guard.config
import cats.derived.derived
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.{Applicative, Endo, Functor}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed private trait ServiceConfigF[X] extends Product derives Functor

private object ServiceConfigF {

  final case class InitParams[K](taskName: Task) extends ServiceConfigF[K]
  final case class WithMetricReport[K](policy: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithMetricReset[K](policy: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithHomepage[K](homepage: Option[Homepage], cont: K) extends ServiceConfigF[K]
  final case class WithTaskName[K](task: Task, cont: K) extends ServiceConfigF[K]
  final case class WithLogFormat[K](format: LogFormat, cont: K) extends ServiceConfigF[K]

  final case class WithRestartPolicy[K](policy: Policy, threshold: Option[Duration], cont: K)
      extends ServiceConfigF[K]

  final case class WithHistoryCapacity[K](panics: Capacity, errors: Capacity, metrics: Capacity, cont: K)
      extends ServiceConfigF[K]

  final case class WithDashboardPolicy[K](policy: Policy, maxPoints: Capacity, cont: K)
      extends ServiceConfigF[K]

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

      case WithRestartPolicy(p, t, c)      => c.focus(_.policies.restart).replace(RestartPolicy(p, t))
      case WithMetricReset(v, c)           => c.focus(_.policies.reset).replace(v)
      case WithMetricReport(p, c)          => c.focus(_.policies.report).replace(p)
      case WithHomepage(v, c)              => c.focus(_.homepage).replace(v)
      case WithTaskName(v, c)              => c.focus(_.taskName).replace(v)
      case WithLogFormat(v, c)             => c.focus(_.logFormat).replace(Some(v))
      case WithHistoryCapacity(p, e, m, c) => c.focus(_.history).replace(Some(HistoryCapacity(p, e, m)))
      case WithDashboardPolicy(p, m, c) => c.focus(_.policies.dashboard).replace(Some(DashboardPolicy(p, m)))
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

  def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(f(Policy), Some(threshold.toJava), cont)))

  def withMetricReport(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReport(f(Policy), cont)))

  def withMetricReset(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReset(f(Policy), cont)))

  def withMetricDailyReset: ServiceConfig[F] =
    withMetricReset(_.crontab(_.daily.midnight))

  def withHomePage(hp: String): ServiceConfig[F] =
    copy(cont = Fix(WithHomepage(Some(Homepage(hp)), cont)))

  def withTaskName(tn: String): ServiceConfig[F] =
    copy(cont = Fix(WithTaskName(Task(tn), cont)))

  def withZoneId(zoneId: ZoneId): ServiceConfig[F] =
    copy(zoneId = zoneId)

  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F] =
    copy(httpBuilder = Some(f))

  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F] = copy(briefs = (fa, briefs).mapN(_.asJson :: _))
  def addBrief[A: Encoder](a: => A): ServiceConfig[F] = addBrief(a.pure[F])

  def withHistoryCapacity(panics: Int, errors: Int, metrics: Int): ServiceConfig[F] =
    copy(cont = Fix(WithHistoryCapacity(Capacity(panics), Capacity(errors), Capacity(metrics), cont)))

  def withLogFormat(f: LogFormat.type => LogFormat): ServiceConfig[F] =
    copy(cont = Fix(WithLogFormat(f(LogFormat), cont)))

  def withInitialAlarmLevel(f: AlarmLevel.type => AlarmLevel): ServiceConfig[F] =
    copy(alarmLevel = f(AlarmLevel))

  def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithDashboardPolicy(f(Policy), Capacity(maxPoints), cont)))

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
