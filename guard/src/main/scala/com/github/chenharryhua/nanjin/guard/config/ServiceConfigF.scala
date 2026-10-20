package com.github.chenharryhua.nanjin.guard.config
import cats.derived.derived
import cats.effect.kernel.Resource
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.{Applicative, Endo, Functor}
import com.github.chenharryhua.nanjin.common.chrono.{zones, Policy}
import com.github.chenharryhua.nanjin.common.logging.{LogLevel, LogLocator}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.trace.TracerProvider

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Immutable service configuration.
  *
  * Each method returns an updated configuration.
  */
sealed trait ServiceConfig[F[_]] {

  /** Set the delay schedule used when restarting after a panic.
    *
    * The schedule resets after the service has run continuously for `threshold`.
    *
    * @param threshold
    *   duration of successful running after which the policy resets
    * @param f
    *   builder for the restart scheduling policy
    */
  def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F]

  /** Set the schedule for collecting and publishing metrics snapshots.
    *
    * A finite policy stops after its scheduled runs; use `.repeat` for ongoing reporting.
    *
    * @param f
    *   builder for the reporting schedule
    */
  def withReportPolicy(f: Policy.type => Policy): ServiceConfig[F]

  /** Configure the output format for all local event logs and the local `ReportedEvent` threshold.
    *
    * Lifecycle and metrics events are not filtered by this threshold.
    *
    * @param f
    *   builder selecting the local `ReportedEvent` threshold
    * @param g
    *   builder selecting the output format, such as console text, JSON, or SLF4J
    */
  def withLog(f: LogLevel.type => LogLevel, g: LogFormat.type => LogFormat): ServiceConfig[F]

  /** Set the minimum level for publishing `ReportedEvent`s to the event channel.
    *
    * @param f
    *   builder selecting the event-channel threshold
    */
  def withReportedEventThreshold(f: LogLevel.type => LogLevel): ServiceConfig[F]

  /** Supply an optional backend log-link locator for service and metrics events.
    *
    * The effect is evaluated when the service starts. When present, the locator creates a link for each event
    * timestamp.
    */
  def withLogLocator(logLocator: F[Option[LogLocator]]): ServiceConfig[F]

  /** Set the time zone used by scheduling and timestamp formatting. */
  def withZoneId(zoneId: ZoneId): ServiceConfig[F]

  /** Set the time zone using the predefined `zones` values. */
  def withZoneId(f: zones.type => ZoneId): ServiceConfig[F]

  /** Set the service homepage URI shown in dashboard and observer output.
    *
    * The string is parsed as a URI; an invalid URI throws during configuration.
    */
  def withHomepage(hp: String): ServiceConfig[F]

  /** Add effectful JSON metadata to service lifecycle events.
    *
    * The value is encoded when the service starts and included with start, panic, and stop events. It is not
    * included with metrics snapshots or reported events.
    */
  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F]

  /** Add by-name JSON metadata to service lifecycle events. */
  def addBrief[A: Encoder](a: => A): ServiceConfig[F]

  /** Enable the embedded HTTP server and configure its Ember server builder.
    *
    * The server provides dashboard and service endpoints; call `withDashboard` to configure the live chart.
    */
  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F]

  /** Configure the dashboard's live WebSocket chart of metered counts over time.
    *
    * The chart retains up to `maxPoints` recent samples per series, dropping older samples as new ones
    * arrive. The chart is served only when the HTTP server is enabled with `withHttpServer`.
    *
    * A value of 1 or less disables the chart but leaves the HTTP server's other routes available.
    *
    * @param maxPoints
    *   maximum number of retained samples per series; 1 or less disables the chart
    * @param f
    *   schedule controlling the chart sampling interval
    */
  def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F]

  /** Set in-memory history capacities for panics, errors, and metrics snapshots.
    *
    * Each history is a ring buffer exposed through the HTTP dashboard. A capacity of zero or less disables
    * that history.
    *
    * @param panics
    *   maximum number of retained panics; zero or less disables the history
    * @param errors
    *   maximum number of retained errors; zero or less disables the history
    * @param metrics
    *   maximum number of retained metrics snapshots; zero or less disables the history
    */
  def withHistoryCapacity(panics: Int, errors: Int, metrics: Int): ServiceConfig[F]

  /** Supply the OpenTelemetry meter provider used to record metrics.
    *
    * The default provider is no-op. A configured provider records instruments from the metrics hub to
    * OpenTelemetry as well as Dropwizard.
    *
    * The provider is a `Resource`, acquired when the service starts and released when it stops.
    *
    * @param meterProvider
    *   resource yielding the meter provider
    */
  def withMeterProvider(meterProvider: Resource[F, MeterProvider[F]]): ServiceConfig[F]

  /** Supply the OpenTelemetry tracer provider used to create tracers.
    *
    * The default provider is no-op. The resource is acquired when the service starts and released when it
    * stops.
    */
  def withTracerProvider(tracerProvider: Resource[F, TracerProvider[F]]): ServiceConfig[F]

}

sealed private trait ServiceConfigF[X] extends Product derives Functor

private object ServiceConfigF {

  final case class InitParams[K](taskName: Task) extends ServiceConfigF[K]
  final case class WithReportPolicy[K](policy: Policy, cont: K) extends ServiceConfigF[K]
  final case class WithHomepage[K](homepage: Option[Homepage], cont: K) extends ServiceConfigF[K]
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
    launchTime: LaunchTime,
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
      case WithReportPolicy(p, c)          => c.focus(_.policies.report).replace(p)
      case WithHomepage(v, c)              => c.focus(_.serviceIdentity.homepage).replace(v)
      case WithLogFormat(v, c)             => c.focus(_.logFormat).replace(Some(v))
      case WithHistoryCapacity(p, e, m, c) => c.focus(_.history).replace(Some(HistoryCapacity(p, e, m)))
      case WithDashboardPolicy(p, m, c) => c.focus(_.policies.dashboard).replace(Some(DashboardPolicy(p, m)))
    }
}

private[guard] object ServiceConfig {

  def apply[F[_]: Applicative](taskName: Task): ServiceConfig[F] =
    Impl[F](
      cont = Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskName)),
      zoneId = ZoneId.systemDefault(),
      httpBuilder = None,
      briefs = List.empty[Json].pure[F],
      logThreshold = LogThreshold(LogLevel.Error, LogLevel.Error),
      logLocator = None.pure[F],
      meterProvider = Resource.pure(MeterProvider.noop[F]),
      tracerProvider = Resource.pure(TracerProvider.noop[F])
    )

  final private[guard] case class Impl[F[_]: Applicative] private[ServiceConfig] (
    cont: Fix[ServiceConfigF],
    zoneId: ZoneId,
    httpBuilder: Option[Endo[EmberServerBuilder[F]]],
    briefs: F[List[Json]],
    logThreshold: LogThreshold,
    logLocator: F[Option[LogLocator]],
    meterProvider: Resource[F, MeterProvider[F]],
    tracerProvider: Resource[F, TracerProvider[F]])
      extends ServiceConfig[F] {

    import ServiceConfigF.*

    override def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F] =
      copy(cont = Fix(WithRestartPolicy(f(Policy), Some(threshold.toJava), cont)))

    override def withReportPolicy(f: Policy.type => Policy): ServiceConfig[F] =
      copy(cont = Fix(WithReportPolicy(f(Policy), cont)))

    override def withHomepage(hp: String): ServiceConfig[F] =
      copy(cont = Fix(WithHomepage(Some(Homepage(hp)), cont)))

    override def withZoneId(zoneId: ZoneId): ServiceConfig[F] =
      copy(zoneId = zoneId)

    override def withZoneId(f: zones.type => ZoneId): ServiceConfig[F] =
      withZoneId(f(zones))

    override def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F] =
      copy(httpBuilder = Some(f))

    override def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F] =
      copy(briefs = (fa, briefs).mapN(_.asJson :: _))

    override def addBrief[A: Encoder](a: => A): ServiceConfig[F] = addBrief(a.pure[F])

    override def withHistoryCapacity(panics: Int, errors: Int, metrics: Int): ServiceConfig[F] =
      copy(cont = Fix(WithHistoryCapacity(Capacity(panics), Capacity(errors), Capacity(metrics), cont)))

    override def withLog(f: LogLevel.type => LogLevel, g: LogFormat.type => LogFormat): ServiceConfig[F] =
      copy(
        cont = Fix(WithLogFormat(g(LogFormat), cont)),
        logThreshold = logThreshold.copy(logger = f(LogLevel)))

    override def withReportedEventThreshold(f: LogLevel.type => LogLevel): ServiceConfig[F] =
      copy(logThreshold = logThreshold.copy(channel = f(LogLevel)))

    override def withLogLocator(logLocator: F[Option[LogLocator]]): ServiceConfig[F] =
      copy(logLocator = logLocator)

    override def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F] =
      copy(cont = Fix(WithDashboardPolicy(f(Policy), Capacity(maxPoints), cont)))

    override def withMeterProvider(meterProvider: Resource[F, MeterProvider[F]]): ServiceConfig[F] =
      copy(meterProvider = meterProvider)

    override def withTracerProvider(tracerProvider: Resource[F, TracerProvider[F]]): ServiceConfig[F] =
      copy(tracerProvider = tracerProvider)

    def evalConfig(
      serviceName: Service,
      serviceId: ServiceId,
      launchTime: LaunchTime,
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
}
