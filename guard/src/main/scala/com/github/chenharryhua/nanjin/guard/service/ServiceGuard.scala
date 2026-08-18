package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Console, Dispatcher, SecureRandom, UUIDGen}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.comcast.ip4s.{ip, port}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.service.dashboard.HttpServer
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.EncoderOps
import org.http4s.ember.server.EmberServerBuilder

import java.util.UUID

/** A guard that manages the lifecycle of a service and produces a stream of observable events.
  *
  * `ServiceGuard` is the primary entry point for running application logic under nanjin's supervision. It
  * wraps user code in a managed context that provides:
  *
  *   - Automatic restart on failure, governed by a configurable restart policy
  *   - Periodic metrics snapshots emitted as events
  *   - Structured lifecycle events (start, panic, stop) for observability
  *   - An optional embedded HTTP dashboard for live inspection
  *
  * The user supplies a callback that receives an [[Agent]], which exposes metrics, logging, scheduling,
  * retry, and circuit-breaker facilities scoped to this service.
  *
  * ===Usage===
  *
  * Obtain a `ServiceGuard` from [[TaskGuard.service]], configure it, then compile its event stream:
  *
  * {{{
  * import cats.effect.IO
  * import com.github.chenharryhua.nanjin.guard.TaskGuard
  * import scala.concurrent.duration.*
  *
  * val task = TaskGuard[IO]("my-task")
  *
  * val events = task
  *   .service("my-service")
  *   .updateConfig(
  *     _.withRestartPolicy(30.seconds, _.fixedDelay(5.seconds).limited(10))
  *       .withMetricsReport(_.crontab(_.hourly))
  *   )
  *   .eventStream { agent =>
  *     // application logic using agent.batch, agent.retry, agent.herald, etc.
  *     agent.tickScheduled(_.fixedDelay(1.minute)).evalMap { tick =>
  *       agent.herald.info(s"heartbeat at \${tick.index}")
  *     }.compile.drain
  *   }
  *
  * // events is a Stream[IO, Event] — compile and drain to run the service
  * events.compile.drain
  * }}}
  *
  * ===Event Stream Variants===
  *
  *   - [[eventStream]]: accepts `Agent[F] => F[Unit]` — the most common form
  *   - [[eventStreamS]]: accepts `Agent[F] => Stream[F, A]` — drains the inner stream automatically
  *   - [[eventStreamR]]: accepts `Agent[F] => Resource[F, A]` — holds the resource open until cancellation
  *
  * All three variants produce the same `Stream[F, Event]` output. The service runs concurrently alongside
  * event emission; observers (SNS, Kafka, database, etc.) can be attached to the event stream via
  * `Translator` and a chosen sink.
  */
sealed trait ServiceGuard[F[_]] extends UpdateConfig[ServiceConfig[F], ServiceGuard[F]] {

  /** Run application logic as `F[Unit]` and produce an event stream.
    *
    * The callback receives an [[Agent]] scoped to this service. When the effect completes normally the
    * service stops; when it fails the restart policy decides whether to retry.
    *
    * @param runAgent
    *   the application logic to execute under supervision
    * @return
    *   a stream of lifecycle, metrics, and reported events
    */
  def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event]

  /** Run application logic as a `Stream` and produce an event stream.
    *
    * The inner stream is compiled to drain; completion or failure triggers the same lifecycle as
    * [[eventStream]].
    */
  def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event]

  /** Hold a `Resource` open for the service lifetime and produce an event stream.
    *
    * The resource is acquired on start and released on stop or cancellation.
    */
  def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event]
}

private[guard] object ServiceGuard {
  def apply[F[_]: {Network, Async, Console}](
    serviceName: Service,
    config: ServiceConfig[F]): ServiceGuard[F] =
    new ServiceGuardImpl[F](serviceName, config)

  final private class ServiceGuardImpl[F[_]: {Network, Console}](
    serviceName: Service,
    config: ServiceConfig[F])(implicit F: Async[F])
      extends ServiceGuard[F] { self =>

    override def updateConfig(f: Endo[ServiceConfig[F]]): ServiceGuard[F] =
      new ServiceGuardImpl[F](serviceName, f(config))

    private case class KickedOff(
      serviceParams: ServiceParams,
      emberServerBuilder: Option[EmberServerBuilder[F]],
      uuidGenerator: F[UUID])

    private def kicking_off: F[KickedOff] =
      SecureRandom.javaSecuritySecureRandom[F].flatMap { implicit sr =>
        for {
          launchTime <- F.realTimeInstant
          jsons <- config.briefs
          serviceId <- UUIDGen.randomUUID[F]
          hostName <- HostName[F]
        } yield {
          val esb: Option[EmberServerBuilder[F]] =
            config.httpBuilder.map(
              _(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026")))

          val params: ServiceParams = config.evalConfig(
            serviceName = serviceName,
            serviceId = ServiceId(serviceId),
            launchTime = launchTime.atZone(config.zoneId),
            brief = Brief(jsons.filterNot(_.isNull).distinct.asJson),
            host = Host(hostName, esb.map(_.port.value).map(Port(_)))
          )
          KickedOff(params, esb, UUIDGen.randomUUID[F])
        }
      }

    override def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event] =
      for {
        KickedOff(serviceParams, emberServerBuilder, uuidGenerator) <- Stream.eval(kicking_off)
        // service level singletons
        dispatcher <- Stream.resource(Dispatcher.sequential[F](await = false))
        channel <- Stream.eval(Channel.unbounded[F, Event])
        logSink = EventLogSink[F](serviceParams)
        seHandler <- ServiceEventHandler(serviceParams, channel, logSink)
        reHandler <- ReportedEventHandler[F](serviceParams, channel, logSink, config.logLevel)
        meHandler <- MetricsEventHandler(serviceParams, channel, logSink)
        agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            channel = channel,
            dispatcher = dispatcher,
            uuidGenerator = uuidGenerator,
            metricsEventHandler = meHandler,
            reportedEventHandler = reHandler
          )
        event <- channel.stream // main stream
          .concurrently(meHandler.reportPeriodically)
          .concurrently(watchdog(F.defer(runAgent(agent)), seHandler))
          .concurrently(
            HttpServer(
              emberServerBuilder = emberServerBuilder,
              metricsEventHandler = meHandler,
              serviceEventHandler = seHandler,
              reportedEventHandler = reHandler
            ))
      } yield event

    override def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event] =
      eventStream(agent => runAgent(agent).compile.drain)

    override def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event] =
      eventStream(agent => runAgent(agent).use_)

  }
}
