package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{AtomicCell, Console, Dispatcher, SecureRandom, UUIDGen}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.catsSyntaxOptionId
import com.codahale.metrics.MetricRegistry
import com.comcast.ip4s.{ip, port}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event}
import com.github.chenharryhua.nanjin.guard.service.dashboard.HttpDataServer
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
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

  final private class ServiceGuardImpl[F[_]: {Network, Async, Console}](
    serviceName: Service,
    config: ServiceConfig[F])
      extends ServiceGuard[F] {
    self =>

    private val F = Async[F]

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
        dispatcher <- Stream.resource(Dispatcher.sequential[F](await = false))
        reQueue = AtomicCell[F].of(new CircularFifoQueue[ReportedEvent](serviceParams.historyCapacity.error))
        errorHistory <- Stream.eval(reQueue)
        alarmLevel <- Stream.eval(Ref.of[F, Option[AlarmLevel]](config.alarmLevel.some))
        channel <- Stream.eval(Channel.unbounded[F, Event])
        metricRegistry: MetricRegistry = new MetricRegistry()
        metricsPublisher <- MetricsPublisher(serviceParams, ScrapeMetrics(metricRegistry), channel)
        lifecyclePublisher <- LifecyclePublisher(serviceParams, channel)
        agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            domain = Domain(serviceParams.serviceName.value),
            metricRegistry = metricRegistry,
            channel = channel,
            errorHistory = errorHistory,
            dispatcher = dispatcher,
            uuidGenerator = uuidGenerator,
            alarmLevel = alarmLevel,
            adhocMetrics = metricsPublisher
          )
        event <- channel.stream // main stream
          .concurrently(metricsPublisher.reset_periodically)
          .concurrently(metricsPublisher.report_periodically)
          .concurrently(Watchdog.stream(F.defer(runAgent(agent)), lifecyclePublisher))
          .concurrently(
            HttpDataServer.stream(
              emberServerBuilder = emberServerBuilder,
              metricsPublisher = metricsPublisher,
              lifecyclePublisher = lifecyclePublisher,
              errorHistory = errorHistory))
      } yield event

    override def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event] =
      eventStream(agent => runAgent(agent).compile.drain)

    override def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event] =
      eventStream(agent => runAgent(agent).use_)

  }
}
