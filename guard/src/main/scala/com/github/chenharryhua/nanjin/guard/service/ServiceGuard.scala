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
import com.github.chenharryhua.nanjin.guard.service.logging.EventLogSink
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.EncoderOps
import org.http4s.ember.server.EmberServerBuilder

import java.util.UUID

sealed trait ServiceGuard[F[_]] extends UpdateConfig[ServiceConfig[F], ServiceGuard[F]] {
  def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event]

  def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event]
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
      SecureRandom.javaSecuritySecureRandom[F].flatMap { sr =>
        given dummy: SecureRandom[F] = sr

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
        logSink <- EventLogSink[F](serviceParams)
        seHandler <- ServiceEventHandler(serviceParams, channel, logSink)
        reHandler <- ReportedEventHandler[F](serviceParams, channel, logSink, config.alarmLevel)
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
          .concurrently(meHandler.resetPeriodically)
          .concurrently(meHandler.reportPeriodically)
          .concurrently(Watchdog(F.defer(runAgent(agent)), seHandler))
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
