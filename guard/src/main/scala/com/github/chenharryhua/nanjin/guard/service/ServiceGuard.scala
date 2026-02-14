package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Console, Dispatcher}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.catsSyntaxOptionId
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{
  AlarmLevel,
  HostName,
  Port,
  ServiceBrief,
  ServiceConfig,
  ServiceName,
  ServiceParams
}
import com.github.chenharryhua.nanjin.guard.event.Event
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.EncoderOps
import org.http4s.ember.server.EmberServerBuilder

sealed trait ServiceGuard[F[_]] extends UpdateConfig[ServiceConfig[F], ServiceGuard[F]] {
  def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event]

  def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event]
  def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event]
}

final private[guard] class ServiceGuardImpl[F[_]: Network: Async: Console] private[guard] (
  serviceName: ServiceName,
  config: ServiceConfig[F])
    extends ServiceGuard[F] { self =>

  private[this] val F = Async[F]

  override def updateConfig(f: Endo[ServiceConfig[F]]): ServiceGuard[F] =
    new ServiceGuardImpl[F](serviceName, f(config))

  private def initStatus: F[(ServiceParams, Option[EmberServerBuilder[F]])] = for {
    jsons <- config.briefs
    zeroth <- Tick.zeroth[F](config.zoneId)
    hostName <- HostName[F]
  } yield {
    val esb: Option[EmberServerBuilder[F]] =
      config.httpBuilder.map(_(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026")))

    val params: ServiceParams = config.evalConfig(
      serviceName = serviceName,
      brief = ServiceBrief(jsons.filterNot(_.isNull).distinct.asJson),
      zerothTick = zeroth,
      hostName = hostName,
      port = esb.map(_.port.value).map(Port(_))
    )
    (params, esb)
  }

  override def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event] =
    for {
      (serviceParams, emberServerBuilder) <- Stream.eval(initStatus)
      dispatcher <- Stream.resource(Dispatcher.sequential[F](await = false))
      helper = new ServiceBuildHelper[F](serviceParams)
      panicHistory <- helper.panic_history
      metricsHistory <- helper.metrics_history
      errorHistory <- helper.error_history
      alarmLevel <- Stream.eval(Ref.of[F, Option[AlarmLevel]](AlarmLevel.Info.some))
      eventLogger <- helper.event_logger(alarmLevel)
      event <- Stream.eval(Channel.unbounded[F, Event]).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()

        val jmx_report: Stream[F, Nothing] =
          config.jmxBuilder match {
            case None        => Stream.empty
            case Some(build) =>
              Stream.bracket(F.blocking {
                val reporter =
                  build(JmxReporter.forRegistry(metricRegistry)) // use home-brew factory
                    .createsObjectNamesWith(objectNameFactory)
                    .build()
                reporter.start()
                reporter
              })(r => F.blocking(r.stop())) >>
                Stream.never[F]
          }

        val http_server: Stream[F, Nothing] =
          emberServerBuilder match {
            case None      => Stream.empty
            case Some(esb) =>
              Stream.resource(
                esb
                  .withHttpApp(new HttpRouter[F](
                    metricRegistry = metricRegistry,
                    panicHistory = panicHistory,
                    metricsHistory = metricsHistory,
                    errorHistory = errorHistory,
                    alarmLevel = alarmLevel,
                    channel = channel,
                    eventLogger = eventLogger
                  ).router)
                  .build) >>
                Stream.never[F]
          }

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            metricRegistry = metricRegistry,
            channel = channel,
            eventLogger = eventLogger,
            errorHistory = errorHistory,
            dispatcher = dispatcher
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F](
            channel = channel,
            eventLogger = eventLogger,
            panicHistory = panicHistory,
            theService = F.defer(runAgent(agent))
          ).stream

        // put together
        channel.stream
          .concurrently(helper.metrics_reset(channel, eventLogger, metricRegistry))
          .concurrently(helper.metrics_report(channel, eventLogger, metricRegistry, metricsHistory))
          .concurrently(jmx_report)
          .concurrently(http_server)
          .concurrently(surveillance)
      }
    } yield event

  def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).compile.drain)

  def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).use_)

}
