package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.Async
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{MetricReport, ServicePanic}
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.*
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.ember.server.EmberServerBuilder


// format: off
/** @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name")
  *       val es: Stream[IO,NJEvent] = guard.eventStream {
  *           gd => gd.action("action-1").retry(IO(1)).run >>
  *                  IO("other computation") >>
  *                  gd.action("action-2").retry(IO(2)).run
  *            }
  * }}}
  */
// format: on

final class ServiceGuard[F[_]: Network] private[guard] (serviceName: ServiceName, config: ServiceConfig[F])(
  implicit F: Async[F])
    extends UpdateConfig[ServiceConfig[F], ServiceGuard[F]] { self =>

  override def updateConfig(f: Endo[ServiceConfig[F]]): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, f(config))

  private lazy val emberServerBuilder: Option[EmberServerBuilder[F]] =
    config.httpBuilder.map(_(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026")))

  private def initStatus: F[ServiceParams] = for {
    jsons <- config.briefs
    zeroth <- Tick.zeroth[F](config.zoneId)
  } yield config.evalConfig(
    serviceName = serviceName,
    emberServerParams = emberServerBuilder.map(EmberServerParams(_)),
    brief = ServiceBrief(jsons.filterNot(_.isNull).distinct.asJson),
    zerothTick = zeroth
  )

  def eventStream[A](runAgent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      panicHistory <- Stream.eval(
        AtomicCell[F].of(new CircularFifoQueue[ServicePanic](serviceParams.historyCapacity.panic)))
      metricsHistory <- Stream.eval(
        AtomicCell[F].of(new CircularFifoQueue[MetricReport](serviceParams.historyCapacity.metric)))
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()

        val metrics_report: Stream[F, Nothing] =
          tickStream[F](
            TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.metricReport))
            .evalMap(tick =>
              publisher
                .metricReport(
                  channel = channel,
                  serviceParams = serviceParams,
                  metricRegistry = metricRegistry,
                  index = MetricIndex.Periodic(tick))
                .flatMap(mr => metricsHistory.modify(queue => (queue, queue.add(mr)))))
            .drain

        val metrics_reset: Stream[F, Nothing] =
          tickStream[F](
            TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.metricReset))
            .evalMap(tick =>
              publisher.metricReset(
                channel = channel,
                serviceParams = serviceParams,
                metricRegistry = metricRegistry,
                index = MetricIndex.Periodic(tick)))
            .drain

        val jmx_report: Stream[F, Nothing] =
          config.jmxBuilder match {
            case None => Stream.empty
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
            case None => Stream.empty
            case Some(builder) =>
              Stream.resource(
                builder
                  .withHttpApp(
                    new HttpRouter[F](
                      metricRegistry = metricRegistry,
                      serviceParams = serviceParams,
                      panicHistory = panicHistory,
                      metricsHistory = metricsHistory,
                      channel = channel).router)
                  .build) >>
                Stream.never[F]
          }

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            metricRegistry = metricRegistry,
            channel = channel,
            measurement = Measurement(serviceParams.serviceName.value)
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F, A](
            channel = channel,
            serviceParams = serviceParams,
            panicHistory = panicHistory,
            theService = F.defer(runAgent(agent))
          ).stream

        // put together
        channel.stream
          .concurrently(metrics_reset)
          .concurrently(metrics_report)
          .concurrently(jmx_report)
          .concurrently(http_server)
          .concurrently(surveillance)
      }
    } yield event
}
