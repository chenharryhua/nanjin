package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Unique}
import cats.effect.std.{AtomicCell, Dispatcher}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import fs2.io.net.Network
import io.circe.Json
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.vault.{Locker, Vault}

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

final class ServiceGuard[F[_]: Network] private[guard] (
  serviceName: ServiceName,
  taskParams: TaskParams,
  config: Endo[ServiceConfig],
  httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  jmxBuilder: Option[Endo[JmxReporter.Builder]],
  brief: F[Option[Json]])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] { self =>

  private def copy(
    serviceName: ServiceName = self.serviceName,
    config: Endo[ServiceConfig] = self.config,
    httpBuilder: Option[EmberServerBuilder[F] => EmberServerBuilder[F]] = self.httpBuilder,
    jmxBuilder: Option[Endo[JmxReporter.Builder]] = self.jmxBuilder,
    brief: F[Option[Json]] = self.brief
  ): ServiceGuard[F] =
    new ServiceGuard[F](
      serviceName = serviceName,
      taskParams = self.taskParams,
      config = config,
      httpBuilder = httpBuilder,
      jmxBuilder = jmxBuilder,
      brief = brief
    )

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] = copy(config = f.compose(self.config))
  def apply(serviceName: String): ServiceGuard[F] = copy(serviceName = ServiceName(serviceName))

  def withHttpServer(f: Endo[EmberServerBuilder[F]]): ServiceGuard[F] = copy(httpBuilder = Some(f))
  def withJmx(f: Endo[JmxReporter.Builder]): ServiceGuard[F]          = copy(jmxBuilder = Some(f))

  def withBrief(json: F[Json]): ServiceGuard[F] = copy(brief = json.map(_.some))
  def withBrief(json: Json): ServiceGuard[F]    = withBrief(F.pure(json))

  private lazy val emberServerBuilder: Option[EmberServerBuilder[F]] =
    httpBuilder.map(_(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026")))

  private def initStatus(zeroth: TickStatus): F[ServiceParams] = for {
    json <- brief
  } yield config(ServiceConfig(taskParams)).evalConfig(
    serviceName = serviceName,
    emberServerParams = emberServerBuilder.map(EmberServerParams(_)),
    brief = ServiceBrief(json),
    zerothTick = zeroth.tick
  )

  def eventStream[A](runAgent: GeneralAgent[F] => F[A]): Stream[F, NJEvent] =
    for {
      zeroth <- Stream.eval(TickStatus.zeroth[F](policies.giveUp, taskParams.zoneId))
      serviceParams <- Stream.eval(initStatus(zeroth))
      signallingMapRef <- Stream.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
      atomicCell <- Stream.eval(AtomicCell[F].of(Vault.empty))
      dispatcher <- Stream.resource(Dispatcher.parallel[F])
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()

        val metricsReport: Stream[F, Nothing] =
          tickStream[F](zeroth.renewPolicy(serviceParams.servicePolicies.metricReport))
            .evalMap(tick =>
              publisher.metricReport(
                channel = channel,
                serviceParams = serviceParams,
                metricRegistry = metricRegistry,
                index = MetricIndex.Periodic(tick),
                ts = tick.wakeup))
            .drain

        val metricsReset: Stream[F, Nothing] =
          tickStream[F](zeroth.renewPolicy(serviceParams.servicePolicies.metricReset))
            .evalMap(tick =>
              publisher.metricReset(
                channel = channel,
                serviceParams = serviceParams,
                metricRegistry = metricRegistry,
                index = MetricIndex.Periodic(tick),
                ts = tick.wakeup))
            .drain

        val jmxReport: Stream[F, Nothing] =
          jmxBuilder match {
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

        val httpServer: Stream[F, Nothing] =
          emberServerBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream.resource(
                builder
                  .withHttpApp(
                    new HttpRouter[F](
                      metricRegistry = metricRegistry,
                      serviceParams = serviceParams,
                      channel = channel).router)
                  .build) >>
                Stream.never[F]
          }

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            metricRegistry = metricRegistry,
            channel = channel,
            signallingMapRef = signallingMapRef,
            atomicCell = atomicCell,
            dispatcher = dispatcher,
            measurement = Measurement(serviceParams.serviceName)
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F, A](
            channel = channel,
            serviceParams = serviceParams,
            theService = runAgent(agent)
          ).stream

        // put together
        channel.stream
          .concurrently(metricsReset)
          .concurrently(metricsReport)
          .concurrently(jmxReport)
          .concurrently(httpServer)
          .concurrently(surveillance)
      }
    } yield event
}
