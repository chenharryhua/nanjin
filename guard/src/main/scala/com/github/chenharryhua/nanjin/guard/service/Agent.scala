package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.common.resilience.{CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.guard.batch.{Batch, BatchId, BatchMetered, BatchTraced, BatchTracer}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricsHub, MetricsHubS}
import fs2.Stream
import fs2.concurrent.Channel
import org.typelevel.otel4s.metrics.Meter as OtelMeter
import org.typelevel.otel4s.trace.{SpanBuilder, SpanOps, Tracer, TracerProvider}

import java.time.ZoneId

/** Scoped service façade for metrics, batching, scheduling, logging, and resilience.
  *
  * An `Agent` is supplied to the callback passed to `ServiceGuard.eventStream`, `eventStreamS`, or
  * `eventStreamR`; it is not normally constructed directly. Use its effectful resources and streams inside
  * that callback, then compile the enclosing event stream:
  *
  * {{{
  * service.eventStream { agent =>
  *   agent.retry(identity).use(_(work)).void
  * }.compile.drain
  * }}}
  *
  * The agent shares the service's zone, event channel, metrics registry, and logging handlers. Derived agents
  * created with `withDomain` keep those shared resources while changing the domain attached to reported
  * events.
  */
sealed trait Agent[F[_]] {

  /** Time zone used by ticks, retry policies, and circuit-breaker policies. */
  val zoneId: ZoneId

  /** The service's otel4s tracer, the same instance that `batchTraced` opens job and batch spans with.
    *
    * Use it to instrument your own effects against the service's tracer so their spans nest as children of
    * the service's spans instead of detached root spans. For a tracer built from the provider rather than
    * this shared instance, see `tracerProvider`.
    */
  def tracer: Tracer[F]

  /** The service's otel4s tracer provider, shared with the service's own job/batch spans.
    *
    * Use it to build HTTP client middleware (or any other instrumentation) against the same otel4s instance
    * the service uses, so their spans nest as children of the service's spans instead of detached root spans.
    */
  def tracerProvider: TracerProvider[F]

  /** Create a view that reports metrics and messages under `name`.
    *
    * The returned agent shares the current agent's service resources; it does not start a new service or
    * create a new metrics registry.
    */
  def withDomain(domain: String): Agent[F]

  /** Create a lightweight batch for a named operation without a metrics hub. */
  def batch(label: String): Batch[F]

  /** Create a metrics-backed batch for a named operation. */
  def batchMetered(label: String): BatchMetered[F]

  /** Create a traced batch for a named operation, running each job in its own OpenTelemetry span.
    *
    * `f` turns the batch's parent span builder into the `SpanOps` that establishes the parent span: it
    * receives a `SpanBuilder` already named `label` and carrying the metric scope's attributes, so the caller
    * only chooses how the parent span is opened (for example `_.build` for a plain span, or a modified
    * builder). Each job then runs in a child span named after the job, nested under that parent.
    *
    * @param label
    *   names the batch, used both as the metric-scope label and the parent span name
    * @param f
    *   builds the parent `SpanOps` from the pre-configured `SpanBuilder`
    */
  def batchTraced(label: String, f: SpanBuilder[F] => SpanOps[F]): BatchTraced[F]

  /** Create a stream of scheduled ticks in the agent's time zone.
    *
    * Use the stream to drive work that follows the policy's schedule.
    *
    * @param f
    *   Function that builds the scheduling policy.
    */
  def tickScheduled(f: Policy.type => Policy): Stream[F, Tick]

  /** Create a stream of future ticks in the agent's time zone.
    *
    * Use this when consumers need the next tick values without the scheduled stream's active timing behavior.
    *
    * @param f
    *   Function that builds the tick policy.
    */
  def tickFuture(f: Policy.type => Policy): Stream[F, Tick]

  /** Logger that writes messages to the log sink and publishes to the event channel.
    *
    * The log sink write is gated by `logThreshold.logger`; the channel publication is gated by
    * `logThreshold.channel`.
    */
  val logger: Log[F]

  /** Create a full metrics hub for a named metric label. */
  def metricsHub(label: String): MetricsHub[F]

  /** Create the stream-based metrics hub for a named metric label.
    *
    * Use the returned `MetricsHubS` when metric registration should compose directly with `fs2.Stream`
    * operations.
    */
  def metricsHubS(label: String): MetricsHubS[F]

  /** Facilitate creating related metric items in one place by applying `f` to a full metrics hub for `label`.
    */
  def facilitate[A](label: String)(f: MetricsHub[F] => A): A

  /** Facilitate creating related stream metrics in one place by applying `f` to the stream-based metrics hub
    * for `label`.
    */
  def facilitateS[A](label: String)(f: MetricsHubS[F] => A): A

  /** Direct access to ad hoc metric reporting for the current service domain. */
  val adhoc: AdhocReport[F]

  /** Create a scoped circuit breaker.
    *
    * Use the returned resource around the protected operation:
    * `agent.circuitBreaker(3, _.fixedDelay(1.second)).use(_.protect(action))`. `maxFailures` is the number of
    * consecutive failures required to transition the breaker from closed to open.
    *
    * A value of `3` opens the breaker after the third consecutive failure while in the closed state.
    *
    * @param maxFailures
    *   Number of consecutive failures required to open the breaker. Must be greater than zero.
    * @param f
    *   Function that builds the policy used for open-to-half-open probe timing.
    *
    * For long-lived breakers, use a non-terminating policy so periodic probe attempts can continue
    * indefinitely.
    */
  def circuitBreaker(maxFailures: Int, f: Policy.type => Policy): Resource[F, CircuitBreaker[F]]

  /** Create a scoped retry interpreter.
    *
    * Use the returned resource around the operation to retry:
    * `agent.retry(_.withPolicy(policy)).use(retry => retry(action))`.
    *
    * @param f
    *   Function that configures the retry builder, including its policy and optional retry decision behavior.
    */
  def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]]

}

final private class GeneralAgent[F[_]: Async](
  val tracer: Tracer[F],
  val tracerProvider: TracerProvider[F],
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  dispatcher: Dispatcher[F],
  batchIdGenerator: F[BatchId],
  metricsEventHandler: MetricsEventHandler[F],
  reportedEventHandler: ReportedEventHandler[F],
  otelMeter: OtelMeter[F]
) extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.serviceIdentity.launchTime.zoneId

  override def withDomain(domain: String): Agent[F] =
    new GeneralAgent[F](
      tracer = tracer,
      tracerProvider = tracerProvider,
      serviceParams = serviceParams,
      channel = channel,
      dispatcher = dispatcher,
      batchIdGenerator = batchIdGenerator,
      metricsEventHandler = metricsEventHandler,
      reportedEventHandler = reportedEventHandler.withDomain(domain),
      otelMeter = otelMeter
    )

  override def tickScheduled(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickScheduled[F](zoneId, f)

  override def tickFuture(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickFuture[F](zoneId, f)

  override def metricsHub(label: String): MetricsHub[F] = {
    val scope = MetricScope(
      MetricScope.Label(label),
      reportedEventHandler.domain,
      serviceParams.serviceIdentity.service,
      serviceParams.serviceIdentity.task)
    MetricsHub[F](scope, metricsEventHandler.metricRegistry, dispatcher, zoneId, otelMeter)
  }

  override def metricsHubS(label: String): MetricsHubS[F] =
    MetricsHubS(metricsHub(label))

  override def facilitate[A](label: String)(f: MetricsHub[F] => A): A =
    f(metricsHub(label))

  override def facilitateS[A](label: String)(f: MetricsHubS[F] => A): A =
    f(metricsHubS(label))

  override def batchMetered(label: String): BatchMetered[F] =
    new BatchMetered[F](log = logger, metrics = metricsHub(label), batchIdGenerator = batchIdGenerator)

  override def batchTraced(label: String, f: SpanBuilder[F] => SpanOps[F]): BatchTraced[F] = {
    val scope = MetricScope(
      MetricScope.Label(label),
      reportedEventHandler.domain,
      serviceParams.serviceIdentity.service,
      serviceParams.serviceIdentity.task)
    new BatchTraced[F](
      scope = scope,
      batchIdGenerator = batchIdGenerator,
      batchTracer = BatchTracer[F](
        tracer = tracer,
        parent = f(tracer.spanBuilder(label).modifyState(_.addAttributes(scope.attributes))))
    )
  }

  override def batch(label: String): Batch[F] = {
    val scope = MetricScope(
      MetricScope.Label(label),
      reportedEventHandler.domain,
      serviceParams.serviceIdentity.service,
      serviceParams.serviceIdentity.task)
    new Batch[F](scope, batchIdGenerator)
  }

  override def circuitBreaker(maxFailures: Int, f: Policy.type => Policy): Resource[F, CircuitBreaker[F]] =
    CircuitBreaker[F](zoneId, maxFailures, f)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    Resource.eval(Retry[F](zoneId, f))

  override val adhoc: AdhocReport[F] = metricsEventHandler

  override val logger: Log[F] = reportedEventHandler.logger
}
