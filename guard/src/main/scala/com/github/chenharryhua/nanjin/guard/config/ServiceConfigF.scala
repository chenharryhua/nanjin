package com.github.chenharryhua.nanjin.guard.config
import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.{Applicative, Endo, Functor}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.jawn.parse
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

import java.time.*
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
@JsonCodec
final case class RestartPolicy(policy: Policy, threshold: Option[Duration])

@JsonCodec
final case class ServicePolicies(restart: RestartPolicy, metricReport: Policy, metricReset: Policy)
@JsonCodec
final case class HistoryCapacity(metric: Int, panic: Int, error: Int)

@JsonCodec
final case class ServiceParams(
  taskName: TaskName,
  hostName: HostName,
  port: Option[Port],
  homePage: Option[HomePage],
  serviceName: ServiceName,
  servicePolicies: ServicePolicies,
  zerothTick: Tick,
  historyCapacity: HistoryCapacity,
  logFormat: LogFormat,
  nanjin: Option[Json],
  brief: Json
) {
  val zoneId: ZoneId = zerothTick.zoneId
  val serviceId: UUID = zerothTick.sequenceId

  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    Instant.EPOCH.plusNanos(fd.toNanos).atZone(zoneId)

  def upTime(ts: ZonedDateTime): Duration = Duration.between(zerothTick.zoned(_.launchTime), ts)
  def upTime(ts: Instant): Duration = Duration.between(zerothTick.launchTime, ts)

  def zonedNow[F[_]: Clock: Functor]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
}

object ServiceParams {

  def apply(
    taskName: TaskName,
    serviceName: ServiceName,
    brief: ServiceBrief,
    zerothTick: Tick,
    hostName: HostName,
    port: Option[Port]
  ): ServiceParams =
    ServiceParams(
      taskName = taskName,
      hostName = hostName,
      port = port,
      homePage = None,
      serviceName = serviceName,
      servicePolicies = ServicePolicies(
        restart = RestartPolicy(Policy.giveUp, None),
        metricReport = Policy.giveUp,
        metricReset = Policy.giveUp),
      zerothTick = zerothTick,
      historyCapacity = HistoryCapacity(32, 32, 32),
      logFormat = LogFormat.Console_PlainText,
      nanjin = parse(BuildInfo.toJson).toOption,
      brief = brief.value
    )
}

sealed private[guard] trait ServiceConfigF[X] extends Product

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskName: TaskName) extends ServiceConfigF[K]

  final case class WithMetricReportPolicy[K](policy: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithRestartPolicy[K](policy: Policy, threshold: Option[Duration], cont: K)
      extends ServiceConfigF[K]
  final case class WithMetricResetPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]

  final case class WithHomePage[K](value: Option[HomePage], cont: K) extends ServiceConfigF[K]

  final case class WithMetricCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]
  final case class WithPanicCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]
  final case class WithErrorCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithTaskName[K](value: TaskName, cont: K) extends ServiceConfigF[K]

  final case class WithLogFormat[K](value: LogFormat, cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    brief: ServiceBrief,
    zerothTick: Tick,
    hostName: HostName,
    port: Option[Port]): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskName) =>
        ServiceParams(
          taskName = taskName,
          serviceName = serviceName,
          brief = brief,
          zerothTick = zerothTick,
          hostName = hostName,
          port = port
        )

      case WithRestartPolicy(p, t, c) =>
        c.focus(_.servicePolicies.restart).replace(RestartPolicy(p, t))
      case WithMetricResetPolicy(v, c)  => c.focus(_.servicePolicies.metricReset).replace(v)
      case WithMetricReportPolicy(p, c) => c.focus(_.servicePolicies.metricReport).replace(p)
      case WithHomePage(v, c)           => c.focus(_.homePage).replace(v)

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

  def withRestartPolicy(threshold: FiniteDuration, restart: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(restart, Some(threshold.toJava), cont)))

  def withRestartPolicy(f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(f(Policy), None, cont)))

  def withMetricReport(policy: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(policy, cont)))

  def withMetricReport(f: Policy.type => Policy): ServiceConfig[F] =
    withMetricReport(policy = f(Policy))

  def withMetricReset(reset: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricResetPolicy(reset, cont)))
  def withMetricReset(f: Policy.type => Policy): ServiceConfig[F] =
    withMetricReset(f(Policy))

  def withMetricDailyReset: ServiceConfig[F] =
    withMetricReset(Policy.crontab(_.daily.midnight))

  def withHomePage(hp: String): ServiceConfig[F] =
    copy(cont = Fix(WithHomePage(Some(HomePage(hp)), cont)))

  def withTaskName(tn: String): ServiceConfig[F] =
    copy(cont = Fix(WithTaskName(TaskName(tn), cont)))

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
    serviceName: ServiceName,
    brief: ServiceBrief,
    zerothTick: Tick,
    hostName: HostName,
    port: Option[Port]): ServiceParams =
    scheme
      .cata(
        algebra(
          serviceName = serviceName,
          brief = brief,
          zerothTick = zerothTick,
          hostName = hostName,
          port = port
        ))
      .apply(cont)
}

private[guard] object ServiceConfig {

  def apply[F[_]: Applicative](taskName: TaskName): ServiceConfig[F] =
    new ServiceConfig[F](
      cont = Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskName)),
      zoneId = ZoneId.systemDefault(),
      jmxBuilder = None,
      httpBuilder = None,
      briefs = List.empty[Json].pure[F]
    )
}
