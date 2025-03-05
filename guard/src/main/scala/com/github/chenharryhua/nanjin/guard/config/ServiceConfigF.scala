package com.github.chenharryhua.nanjin.guard.config
import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.{Applicative, Endo, Functor}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder

import java.time.*
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class ServicePolicies(restart: Policy, metricReport: Policy, metricReset: Policy)
@JsonCodec
final case class HistoryCapacity(metric: Int, panic: Int)

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
  zerothTick: Tick,
  historyCapacity: HistoryCapacity,
  brief: Json
) {
  val zoneId: ZoneId  = zerothTick.zoneId
  val serviceId: UUID = zerothTick.sequenceId

  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    Instant.EPOCH.plusNanos(fd.toNanos).atZone(zoneId)

  def upTime(ts: ZonedDateTime): Duration = Duration.between(zerothTick.zonedLaunchTime, ts)
  def upTime(ts: Instant): Duration       = Duration.between(zerothTick.launchTime, ts)

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
      servicePolicies =
        ServicePolicies(restart = Policy.giveUp, metricReport = Policy.giveUp, metricReset = Policy.giveUp),
      emberServerParams = emberServerParams,
      threshold = None,
      zerothTick = zerothTick,
      historyCapacity = HistoryCapacity(32, 32),
      brief = brief.value
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

  final case class WithMetricCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]
  final case class WithPanicCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]

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

      case WithMetricCapacity(v, c) => c.focus(_.historyCapacity.metric).replace(v)
      case WithPanicCapacity(v, c)  => c.focus(_.historyCapacity.panic).replace(v)
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

  def withRestartThreshold(fd: FiniteDuration): ServiceConfig[F] =
    copy(cont = Fix(WithRestartThreshold(Some(fd.toJava), cont)))

  def withRestartPolicy(restart: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithRestartPolicy(restart, cont)))
  def withRestartPolicy(f: Policy.type => Policy): ServiceConfig[F] =
    withRestartPolicy(f(Policy))

  def withMetricReport(report: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricReportPolicy(report, cont)))
  def withMetricReport(f: Policy.type => Policy): ServiceConfig[F] =
    withMetricReport(f(Policy))

  def withMetricReset(reset: Policy): ServiceConfig[F] =
    copy(cont = Fix(WithMetricResetPolicy(reset, cont)))
  def withMetricReset(f: Policy.type => Policy): ServiceConfig[F] =
    withMetricReset(f(Policy))

  def withMetricDailyReset: ServiceConfig[F] =
    withMetricReset(Policy.crontab(_.daily.midnight))

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

  def withPanicHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithPanicCapacity(value, cont)))
  def withMetricHistoryCapacity(value: Int): ServiceConfig[F] =
    copy(cont = Fix(WithMetricCapacity(value, cont)))

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
