package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{AtomicCell, Console, Dispatcher}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricReport, ServiceMessage, ServicePanic}
import com.github.chenharryhua.nanjin.guard.translator.Translator
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.net.Network
import io.circe.syntax.*
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.SelfAwareLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class ServiceGuard[F[_]: Network: Async: Console] private[guard] (
  serviceName: ServiceName,
  config: ServiceConfig[F])
    extends UpdateConfig[ServiceConfig[F], ServiceGuard[F]] { self =>

  private[this] val F = Async[F]

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

  private def initAlarmLevel(log: SelfAwareLogger[F]): F[Option[AlarmLevel]] =
    (log.isTraceEnabled, log.isDebugEnabled, log.isInfoEnabled, log.isWarnEnabled, log.isErrorEnabled).mapN {
      case (trace, debug, info, warn, error) =>
        if (trace) AlarmLevel.Debug.some
        else if (debug) AlarmLevel.Debug.some
        else if (info) AlarmLevel.Info.some
        else if (warn) AlarmLevel.Warn.some
        else if (error) AlarmLevel.Error.some
        else none[AlarmLevel]
    }

  def eventStream(runAgent: Agent[F] => F[Unit]): Stream[F, Event] =
    for {
      serviceParams <- Stream.eval(initStatus)
      dispatcher <- Stream.resource(Dispatcher.sequential[F](await = false))
      panicHistory <- Stream.eval(
        AtomicCell[F].of(new CircularFifoQueue[ServicePanic](serviceParams.historyCapacity.panic)))
      metricsHistory <- Stream.eval(
        AtomicCell[F].of(new CircularFifoQueue[MetricReport](serviceParams.historyCapacity.metric)))
      errorHistory <- Stream.eval(
        AtomicCell[F].of(new CircularFifoQueue[ServiceMessage](serviceParams.historyCapacity.error)))
      logger <- Stream.eval(Slf4jLogger.fromName[F](serviceName.value))
      alarmLevel <- Stream.eval(initAlarmLevel(logger).flatMap(Ref.of[F, Option[AlarmLevel]]))
      event <- Stream.eval(Channel.unbounded[F, Event]).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()
        val eventLogger: EventLogger[F] = serviceParams.logFormat match {
          case LogFormat.Console =>
            new EventLogger[F](SimpleTextTranslator[F], new ConsoleLogger[F](serviceParams.zoneId))
          case LogFormat.PlainText    => new EventLogger[F](SimpleTextTranslator[F], logger)
          case LogFormat.JsonNoSpaces => new EventLogger[F](PrettyJsonTranslator[F].map(_.noSpaces), logger)
          case LogFormat.JsonSpaces2  => new EventLogger[F](PrettyJsonTranslator[F].map(_.spaces2), logger)
          case LogFormat.JsonVerbose  =>
            new EventLogger[F](Translator.idTranslator.map(_.asJson.spaces2), logger)
        }

        val metrics_report: Stream[F, Nothing] =
          tickStream
            .fromTickStatus[F](TickStatus(serviceParams.zerothTick).renewPolicy(
              serviceParams.servicePolicies.metricReport.policy))
            .evalMap { tick =>
              metricReport(
                channel = channel,
                eventLogger = eventLogger,
                serviceParams = serviceParams,
                metricRegistry = metricRegistry,
                index = MetricIndex.Periodic(tick)).flatMap(mr =>
                metricsHistory.modify(queue => (queue, queue.add(mr))))
            }
            .drain

        val metrics_reset: Stream[F, Nothing] =
          tickStream
            .fromTickStatus[F](
              TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.metricReset))
            .evalMap(tick =>
              metricReset(
                channel = channel,
                eventLogger = eventLogger,
                serviceParams = serviceParams,
                metricRegistry = metricRegistry,
                index = MetricIndex.Periodic(tick)))
            .drain

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
            case None          => Stream.empty
            case Some(builder) =>
              Stream.resource(
                builder
                  .withHttpApp(new HttpRouter[F](
                    metricRegistry = metricRegistry,
                    serviceParams = serviceParams,
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
            serviceParams = serviceParams,
            metricRegistry = metricRegistry,
            channel = channel,
            eventLogger = eventLogger,
            domain = Domain(serviceParams.serviceName.value),
            alarmLevel = alarmLevel,
            errorHistory = errorHistory,
            dispatcher = dispatcher
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F](
            channel = channel,
            eventLogger = eventLogger,
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

  def eventStreamS[A](runAgent: Agent[F] => Stream[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).compile.drain)

  def eventStreamR[A](runAgent: Agent[F] => Resource[F, A]): Stream[F, Event] =
    eventStream(agent => runAgent(agent).use_)

}
