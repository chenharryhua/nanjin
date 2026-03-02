package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Console, Dispatcher, SecureRandom, UUIDGen}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.catsSyntaxOptionId
import com.codahale.metrics.MetricRegistry
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{
  AlarmLevel,
  Brief,
  Host,
  HostName,
  Port,
  Service,
  ServiceConfig,
  ServiceId,
  ServiceParams
}
import com.github.chenharryhua.nanjin.guard.dashboard.DashboardWs
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event}
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

final private[guard] class ServiceGuardImpl[F[_]: Network: Async: Console] private[guard] (
  serviceName: Service,
  config: ServiceConfig[F])
    extends ServiceGuard[F] { self =>

  private[this] val F = Async[F]

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
          config.httpBuilder.map(_(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026")))

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
      kickedOff <- Stream.eval(kicking_off)
      dispatcher <- Stream.resource(Dispatcher.sequential[F](await = false))
      helper = new ServiceBuildHelper[F](kickedOff.serviceParams)
      errorHistory <- helper.error_history
      alarmLevel <- Stream.eval(Ref.of[F, Option[AlarmLevel]](config.alarmLevel.some))
      logSink <- helper.service_log_sink
      channel <- Stream.eval(Channel.unbounded[F, Event])
      metricRegistry: MetricRegistry = new MetricRegistry()
      metricsPublisher <- MetricsPublisher(kickedOff.serviceParams, metricRegistry, channel, logSink)
      lifecyclePublisher <- LifecyclePublisher(kickedOff.serviceParams, channel, logSink)
      event <- {
//        val http_server: Stream[F, Nothing] =
//          kickedOff.emberServerBuilder.fold(Stream.empty.covaryAll[F, Nothing]) { esb =>
//            val router = new HttpRouter[F](
//              serviceParams = kickedOff.serviceParams,
//              errorHistory = errorHistory,
//              alarmLevel = alarmLevel,
//              metricsPublisher = metricsPublisher,
//              lifecyclePublisher = lifecyclePublisher
//            ).router
//
//            Stream.resource(esb.withHttpApp(router).build) >> Stream.never[F]
//          }

        val dashboard_server: Stream[F, Nothing] =
          kickedOff.emberServerBuilder.fold(Stream.empty.covaryAll[F, Nothing]) { esb =>
            val ws = new DashboardWs[F](
              port = esb.port,
              serviceParams = kickedOff.serviceParams,
              metricRegistry = metricRegistry
            )

            Stream.resource(esb.withHttpWebSocketApp(ws.dashboardRouter).build) >> Stream.never[F]
          }

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            kickedOff.serviceParams,
            Domain(kickedOff.serviceParams.serviceName.value),
            metricRegistry = metricRegistry,
            channel = channel,
            errorHistory = errorHistory,
            dispatcher = dispatcher,
            uuidGenerator = kickedOff.uuidGenerator,
            alarmLevel = alarmLevel,
            adhocMetrics = metricsPublisher
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F](
            serviceParams = kickedOff.serviceParams,
            theService = F.defer(runAgent(agent)),
            lifecyclePublisher = lifecyclePublisher
          ).stream

        // put together
        channel.stream
          .concurrently(metricsPublisher.reset_periodic)
          .concurrently(metricsPublisher.report_periodic)
          .concurrently(helper.service_jmx_report(metricRegistry, config.jmxBuilder))
          .concurrently(dashboard_server)
          .concurrently(surveillance)
      }
    } yield event

  override def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).compile.drain)

  override def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).use_)

}
