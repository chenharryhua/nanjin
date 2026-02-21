package com.github.chenharryhua.nanjin.guard.config
import cats.effect.kernel.Clock
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.functor.toFunctorOps
import cats.{Applicative, Endo, Functor, Show}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.chrono.Policy
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.jawn.parse
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class RestartPolicy(policy: Policy, threshold: Option[Duration])

@JsonCodec
final case class ServicePolicies(restart: RestartPolicy, metricsReport: Policy, metricsReset: Policy)
@JsonCodec
final case class HistoryCapacity(metric: Int, panic: Int, error: Int)

@JsonCodec
final case class Host(name: HostName, port: Option[Port]) {
  override def toString: String =
    port match {
      case Some(p) => s"${name.value}:${p.value}"
      case None    => name.value
    }
}
object Host {
  implicit val showHost: Show[Host] = Show.fromToString[Host]
}

@JsonCodec
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
) {
  val zoneId: ZoneId = launchTime.getZone
  val timeZone: TimeZone = TimeZone(zoneId)

  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    Instant.EPOCH.plusNanos(fd.toNanos).atZone(zoneId)

  def upTime(ts: ZonedDateTime): UpTime = UpTime(Duration.between(launchTime, ts))
  def upTime(ts: Instant): UpTime = UpTime(Duration.between(launchTime.toInstant, ts))

  def zonedNow[F[_]: Clock: Functor]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
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
      servicePolicies = ServicePolicies(
        restart = RestartPolicy(Policy.empty, None),
        metricsReport = Policy.empty,
        metricsReset = Policy.empty),
      historyCapacity = HistoryCapacity(32, 32, 32),
      logFormat = LogFormat.Console_PlainText,
      nanjin = parse(BuildInfo.toJson).toOption,
      brief = brief
    )
}

sealed private[guard] trait ServiceConfigF[X] extends Product

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskName: Task) extends ServiceConfigF[K]

  final case class WithMetricReportPolicy[K](policy: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithRestartPolicy[K](policy: Policy, threshold: Option[Duration], cont: K)
      extends ServiceConfigF[K]
  final case class WithMetricResetPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]

  final case class WithHomePage[K](value: Option[Homepage], cont: K) extends ServiceConfigF[K]

  final case class WithMetricCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]
  final case class WithPanicCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]
  final case class WithErrorCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithTaskName[K](value: Task, cont: K) extends ServiceConfigF[K]

  final case class WithLogFormat[K](value: LogFormat, cont: K) extends ServiceConfigF[K]

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

      case WithRestartPolicy(p, t, c) =>
        c.focus(_.servicePolicies.restart).replace(RestartPolicy(p, t))
      case WithMetricResetPolicy(v, c)  => c.focus(_.servicePolicies.metricsReset).replace(v)
      case WithMetricReportPolicy(p, c) => c.focus(_.servicePolicies.metricsReport).replace(p)
      case WithHomePage(v, c)           => c.focus(_.homepage).replace(v)

      case WithMetricCapacity(v, c) => c.focus(_.historyCapacity.metric).replace(v)
      case WithPanicCapacity(v, c)  => c.focus(_.historyCapacity.panic).replace(v)
      case WithErrorCapacity(v, c)  => c.focus(_.historyCapacity.error).replace(v)

      case WithTaskName(v, c) => c.focus(_.taskName).replace(v)

      case WithLogFormat(v, c) => c.focus(_.logFormat).replace(v)
    }
}

final class ServiceConfig[F[_]: Applicative] private (
  cont: Fix[ServiceConfigF],
  private[guard] val zoneId: ZoneId,
  private[guard] val jmxBuilder: Option[Endo[JmxReporter.Builder]],
  private[guard] val httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  private[guard] val briefs: F[List[Json]]) {
  import ServiceConfigF.*

  private def copy(
    cont: Fix[ServiceConfigF] = this.cont,
    zoneId: ZoneId = this.zoneId,
    jmxBuilder: Option[Endo[JmxReporter.Builder]] = this.jmxBuilder,
    httpBuilder: Option[Endo[EmberServerBuilder[F]]] = this.httpBuilder,
    briefs: F[List[Json]] = this.briefs): ServiceConfig[F] =
    new ServiceConfig[F](cont, zoneId, jmxBuilder, httpBuilder, briefs)

  def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(f(Policy), Some(threshold.toJava), cont)))

  def withMetricReport(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(f(Policy), cont)))

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

  def withJmx(f: Endo[JmxReporter.Builder]): ServiceConfig[F] =
    copy(jmxBuilder = Some(f))
  def disableJmx: ServiceConfig[F] =
    copy(jmxBuilder = None)

  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F] =
    copy(httpBuilder = Some(f))
  def disableHttpServer: ServiceConfig[F] =
    copy(httpBuilder = None)

  def withPanicHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithPanicCapacity(value, cont)))
  def withMetricHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithMetricCapacity(value, cont)))
  def withErrorHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithErrorCapacity(value, cont)))

  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F] = copy(briefs = (fa, briefs).mapN(_.asJson :: _))
  def addBrief[A: Encoder](a: => A): ServiceConfig[F] = addBrief(a.pure[F])

  def withLogFormat(fmt: LogFormat): ServiceConfig[F] =
    copy(cont = Fix(WithLogFormat(fmt, cont)))
  def withLogFormat(f: LogFormat.type => LogFormat): ServiceConfig[F] =
    withLogFormat(f(LogFormat))

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
      jmxBuilder = None,
      httpBuilder = None,
      briefs = List.empty[Json].pure[F]
    )
}
