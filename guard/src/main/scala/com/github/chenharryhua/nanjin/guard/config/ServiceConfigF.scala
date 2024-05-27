package com.github.chenharryhua.nanjin.guard.config
import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.{Applicative, Endo, Functor}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy, Tick}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.{Encoder, Json}
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

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
  taskName: TaskName,
  hostName: HostName,
  homePage: Option[HomePage],
  serviceName: ServiceName,
  servicePolicies: ServicePolicies,
  emberServerParams: Option[EmberServerParams],
  threshold: Option[Duration],
  brief: Json,
  zerothTick: Tick
) {
  val zoneId: ZoneId  = zerothTick.zoneId
  val serviceId: UUID = zerothTick.sequenceId

  val launchTime: ZonedDateTime = zerothTick.launchTime.atZone(zoneId)

  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    Instant.EPOCH.plusNanos(fd.toNanos).atZone(zoneId)

  def upTime(ts: ZonedDateTime): Duration = Duration.between(launchTime, ts)

  def zonedNow[F[_]: Clock: Functor]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
}

object ServiceParams {

  def apply(
    taskName: TaskName,
    serviceName: ServiceName,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zerothTick: Tick
  ): ServiceParams =
    ServiceParams(
      taskName = taskName,
      hostName = HostName.local_host,
      homePage = None,
      serviceName = serviceName,
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

sealed private[guard] trait ServiceConfigF[X] extends Product with Serializable

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskName: TaskName) extends ServiceConfigF[K]

  final case class WithRestartThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]
  final case class WithRestartPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithMetricReportPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithMetricResetPolicy[K](value: Policy, cont: K) extends ServiceConfigF[K]

  final case class WithHostName[K](value: HostName, cont: K) extends ServiceConfigF[K]
  final case class WithHomePage[K](value: Option[HomePage], cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    emberServerParams: Option[EmberServerParams],
    brief: ServiceBrief,
    zerothTick: Tick): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskName) =>
        ServiceParams(
          taskName = taskName,
          serviceName = serviceName,
          emberServerParams = emberServerParams,
          brief = brief,
          zerothTick = zerothTick
        )

      case WithRestartThreshold(v, c)   => c.focus(_.threshold).replace(v)
      case WithRestartPolicy(v, c)      => c.focus(_.servicePolicies.restart).replace(v)
      case WithMetricReportPolicy(v, c) => c.focus(_.servicePolicies.metricReport).replace(v)
      case WithMetricResetPolicy(v, c)  => c.focus(_.servicePolicies.metricReset).replace(v)
      case WithHostName(v, c)           => c.focus(_.hostName).replace(v)
      case WithHomePage(v, c)           => c.focus(_.homePage).replace(v)
    }
}

final case class ServiceConfig[F[_]: Applicative](
  private[guard] val cont: Fix[ServiceConfigF],
  private[guard] val zoneId: ZoneId,
  private[guard] val jmxBuilder: Option[Endo[JmxReporter.Builder]],
  private[guard] val httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  private[guard] val briefs: F[List[Json]]) {
  import ServiceConfigF.*

  def withRestartThreshold(fd: FiniteDuration): ServiceConfig[F] =
    copy(cont = Fix(WithRestartThreshold(Some(fd.toJava), cont)))

  def withRestartPolicy(restart: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(restart, cont)))

  def withMetricReport(report: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(report, cont)))

  def withMetricReset(reset: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricResetPolicy(reset, cont)))

  def withMetricDailyReset: ServiceConfig[F] =
    withMetricReset(policies.crontab(_.daily.midnight))

  def withHostName(hostName: HostName): ServiceConfig[F] =
    copy(cont = Fix(WithHostName(hostName, cont)))

  def withHomePage(hp: String): ServiceConfig[F] =
    copy(cont = Fix(WithHomePage(Some(HomePage(hp)), cont)))

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

  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F] = copy(briefs = (fa, briefs).mapN(_.asJson :: _))
  def addBrief[A: Encoder](a: => A): ServiceConfig[F]  = addBrief(a.pure[F])

  private[guard] def evalConfig(
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

  def apply[F[_]: Applicative](taskName: TaskName): ServiceConfig[F] =
    new ServiceConfig[F](
      cont = Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskName)),
      zoneId = ZoneId.systemDefault(),
      jmxBuilder = None,
      httpBuilder = None,
      briefs = List.empty[Json].pure[F]
    )
}
