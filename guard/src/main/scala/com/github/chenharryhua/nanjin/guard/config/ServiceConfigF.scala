package com.github.chenharryhua.nanjin.guard.config
import cats.derived.derived
import cats.effect.kernel.Resource
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.{Applicative, Endo, Functor}
import com.github.chenharryhua.nanjin.common.chrono.{zones, Policy}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.otel4s.metrics.MeterProvider

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait ServiceConfig[F[_]] {

  /** Set the restart policy for the service.
    *
    * When the service crashes, the policy governs retry delays. If the service has been running longer than
    * `threshold` since the last panic, the policy resets to its initial state.
    *
    * @param threshold
    *   duration of successful running after which the policy resets
    * @param f
    *   builder for the restart scheduling policy
    */
  def withRestartPolicy(threshold: FiniteDuration, f: Policy.type => Policy): ServiceConfig[F]

  /** Set the periodic metrics reporting policy.
    *
    * The policy drives how often metrics snapshots are collected and published as events. Use `.repeat` to
    * keep reporting indefinitely.
    *
    * @param f
    *   builder for the reporting schedule
    */
  def withReportPolicy(f: Policy.type => Policy): ServiceConfig[F]

  /** Set the log output format (console plain text, JSON, SLF4J, etc.). */
  def withLogFormat(f: LogFormat.type => LogFormat): ServiceConfig[F]

  /** Set the minimum log levels for the two logging paths.
    *
    * @param logger
    *   threshold for the local log sink (e.g. console/file). Messages below this level are not written.
    * @param channel
    *   threshold for the event channel (observers, alerts). Messages below this level are not published.
    */
  def withLogThreshold(
    logger: LogLevel.type => LogLevel,
    channel: LogLevel.type => LogLevel): ServiceConfig[F]

  /** Set the time zone used by ticks, policies, and timestamp formatting. */
  def withZoneId(zoneId: ZoneId): ServiceConfig[F]

  /** Set the time zone using a builder function over the predefined `zones` object. */
  def withZoneId(f: zones.type => ZoneId): ServiceConfig[F]

  /** Set the service homepage URL, shown in dashboard and observer output. */
  def withHomepage(hp: String): ServiceConfig[F]

  /** Attach an effectful brief to the service metadata.
    *
    * Briefs are JSON documents that travel with the service lifecycle events (start, panic, stop); they are
    * not attached to metrics-snapshot or reported events. Use them for deployment context, build info, or
    * custom annotations.
    */
  def addBrief[A: Encoder](fa: F[A]): ServiceConfig[F]

  /** Attach a by-name brief to the service metadata. */
  def addBrief[A: Encoder](a: => A): ServiceConfig[F]

  /** Enable the embedded HTTP dashboard server with a custom Ember server builder.
    *
    * The server exposes REST endpoints for metrics, params, health checks, log level control, and optionally
    * a WebSocket-based live chart.
    */
  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceConfig[F]

  /** Enable the live WebSocket dashboard with a chart showing metered counts over time.
    *
    * `maxPoints` bounds how many recent data points the chart holds per series: as new points arrive, the
    * oldest are dropped, so the chart shows a sliding window of roughly the last `maxPoints` samples. Larger
    * values show more history at the cost of a denser chart.
    *
    * A `maxPoints` of 1 or less disables the live chart: the dashboard and its other routes still serve, but
    * no WebSocket chart is mounted (a single point is not a meaningful time series).
    *
    * @param maxPoints
    *   size of the chart's sliding window per series; 1 or less disables the chart.
    * @param f
    *   policy controlling how often data points are sampled.
    */
  def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F]

  /** Set the bounded history capacity for panics, errors, and metrics snapshots.
    *
    * These histories are accessible via the HTTP dashboard and are kept in-memory as ring buffers. A capacity
    * of zero or negative disables the corresponding history: it retains nothing and always reports empty.
    *
    * @param panics
    *   capacity of the panic history; zero or negative disables it.
    * @param errors
    *   capacity of the error history; zero or negative disables it.
    * @param metrics
    *   capacity of the metrics-snapshot history; zero or negative disables it.
    */
  def withHistoryCapacity(panics: Int, errors: Int, metrics: Int): ServiceConfig[F]

  /** Supply an OpenTelemetry `org.typelevel.otel4s.metrics.MeterProvider` for recording metrics.
    *
    * By default, the provider is a no-op, so otel4s metrics are disabled and incur no cost. Configuring a
    * real provider opts the service into emitting metrics through the standard metrics hub: instruments
    * created via `agent.facilitate`/`agent.metricsHub` then record to both Dropwizard and otel4s.
    *
    * The provider is supplied as a `Resource`: the SDK-backed `MeterProvider` owns exporters and background
    * readers, so the service acquires it on start and releases (flushes/shuts down) it on stop.
    *
    * ===Resource attributes===
    * nanjin stamps only per-metric dimensions (`domain`, `category`) onto each measurement. Emitter identity
    * belongs on the OpenTelemetry Resource, which is owned by the SDK and cannot be set from here. When
    * building the `MeterProvider`, set these standard Resource attributes so metrics carry the service
    * identity:
    *   - `service.name` ← this service's name
    *   - `service.namespace` ← the task name (the grouping above services)
    *   - `service.instance.id` ← the service id (a new value per deployment)
    *
    * @param meterProvider
    *   a resource yielding the otel4s meter provider used to create meters and instruments
    */
  def withMeterProvider(meterProvider: Resource[F, MeterProvider[F]]): ServiceConfig[F]
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

final private[guard] case class ServiceConfigImpl[F[_]: Applicative](
  cont: Fix[ServiceConfigF],
  zoneId: ZoneId,
  httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  briefs: F[List[Json]],
  logThreshold: LogThreshold,
  meterProvider: Resource[F, MeterProvider[F]])
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

  override def withLogFormat(f: LogFormat.type => LogFormat): ServiceConfig[F] =
    copy(cont = Fix(WithLogFormat(f(LogFormat), cont)))

  override def withLogThreshold(
    logger: LogLevel.type => LogLevel,
    channel: LogLevel.type => LogLevel): ServiceConfig[F] =
    copy(logThreshold = LogThreshold(logger(LogLevel), channel(LogLevel)))

  override def withDashboard(maxPoints: Int, f: Policy.type => Policy): ServiceConfig[F] =
    copy(cont = Fix(WithDashboardPolicy(f(Policy), Capacity(maxPoints), cont)))

  override def withMeterProvider(meterProvider: Resource[F, MeterProvider[F]]): ServiceConfig[F] =
    copy(meterProvider = meterProvider)

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

private[guard] object ServiceConfig {

  def apply[F[_]: Applicative](taskName: Task): ServiceConfig[F] =
    ServiceConfigImpl[F](
      cont = Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskName)),
      zoneId = ZoneId.systemDefault(),
      httpBuilder = None,
      briefs = List.empty[Json].pure[F],
      logThreshold = LogThreshold(LogLevel.Info, LogLevel.Warn),
      meterProvider = Resource.pure(MeterProvider.noop[F])
    )
}
