package com.github.chenharryhua.nanjin.guard

import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricRegistry, MetricSet}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.concurrent.Channel
import fs2.{INothing, Stream}

import java.time.{Duration, ZonedDateTime}
import scala.compat.java8.DurationConverters.DurationOps
import scala.concurrent.duration.FiniteDuration

// format: off
/** @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") 
  *       val es: Stream[IO,NJEvent] = guard.eventStream {
  *           gd => gd("action-1").retry(IO(1)).run >> 
  *                  IO("other computation") >> 
  *                  gd("action-2").retry(IO(2)).run 
  *            }
  * }}}
  */
// format: on

final class ServiceGuard[F[_]] private[guard] (
  metricRegistry: MetricRegistry,
  serviceConfig: ServiceConfig,
  jmxBuilder: Option[Reader[JmxReporter.Builder, JmxReporter.Builder]])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  lazy val params: ServiceParams = serviceConfig.evalConfig

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, f(serviceConfig), jmxBuilder)

  def withJmxReporter(builder: JmxReporter.Builder => JmxReporter.Builder): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, serviceConfig, Some(Reader(builder)))

  def registerMetricSet(ms: MetricSet): ServiceGuard[F] = {
    metricRegistry.registerAll(ms)
    this
  }

  def eventStream[A](actionGuard: ActionGuard[F] => F[A]): Stream[F, NJEvent] = {
    val serviceInfo: F[ServiceInfo] = for {
      ts <- realZonedDateTime(params)
      uuid <- UUIDGen.randomUUID
    } yield ServiceInfo(id = uuid, launchTime = ts)

    val mrService: NJMetricRegistry = new NJMetricRegistry(metricRegistry)

    for {
      si <- Stream.eval(serviceInfo)
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val theService: F[A] = retry.mtl
          .retryingOnAllErrors(
            params.retry.policy[F],
            (ex: Throwable, rd) =>
              realZonedDateTime(params).flatMap(ts =>
                channel
                  .send(
                    ServicePanic(
                      timestamp = ts,
                      serviceInfo = si,
                      serviceParams = params,
                      retryDetails = rd,
                      error = NJError(ex)))
                  .void)
          ) {
            val startUp = realZonedDateTime(params).flatMap(ts =>
              channel.send(ServiceStarted(timestamp = ts, serviceInfo = si, serviceParams = params)))

            startUp *> Dispatcher[F].use(dispatcher =>
              actionGuard(
                new ActionGuard[F](
                  metricRegistry = metricRegistry,
                  dispatcher = dispatcher,
                  channel = channel,
                  actionConfig = ActionConfig(params))))
          }
          .guarantee(realZonedDateTime(params).flatMap(ts =>
            channel.send(ServiceStopped(timestamp = ts, serviceInfo = si, serviceParams = params))) *> // stop event
            channel.close.void) // close channel and the stream as well

        /** concurrent streams
          */

        // metrics report
        val reporting: Stream[F, INothing] = {
          def report(ts: ZonedDateTime, dur: Option[FiniteDuration]) = MetricsReport(
            timestamp = ts,
            serviceInfo = si,
            serviceParams = params,
            next = dur,
            metrics = MetricRegistryWrapper(Some(metricRegistry))
          )

          params.reportingSchedule match {
            case Left(dur) =>
              Stream
                .fixedRate[F](dur)
                .evalMap(_ => realZonedDateTime(params).map(ts => report(ts, Some(dur))).flatMap(channel.send))
                .drain
            case Right(cron) =>
              val scheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))
              scheduler
                .awakeEvery(cron)
                .evalMap { _ =>
                  realZonedDateTime(params).map { ts =>
                    report(ts, cron.next(ts).map(zd => Duration.between(ts, zd).toScala))
                  }.flatMap(channel.send)
                }
                .drain
          }
        }

        val jmxReporting: Stream[F, INothing] = {
          jmxBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream
                .bracket(F.delay(builder.run(JmxReporter.forRegistry(metricRegistry)).build()))(r => F.delay(r.close()))
                .evalMap(jr => F.delay(jr.start()))
                .flatMap(_ => Stream.never[F])
          }
        }

        // put together

        val accessories: Stream[F, INothing] =
          Stream(reporting, jmxReporting, Stream.eval(theService).drain).parJoinUnbounded

        channel.stream.evalTap(mrService.compute[F]).concurrently(accessories)
      }
    } yield event
  }
}

final private class NJMetricRegistry(registry: MetricRegistry) {

  def compute[F[_]](event: NJEvent)(implicit F: Sync[F]): F[Unit] = event match {
    // counters
    case _: MetricsReport          => F.delay(registry.counter("01.health.check").inc())
    case _: ServiceStarted         => F.delay(registry.counter("02.service.start").inc())
    case _: ServiceStopped         => F.delay(registry.counter("03.service.stop").inc())
    case _: ServicePanic           => F.delay(registry.counter("04.service.`panic`").inc())
    case _: ForYourInformation     => F.delay(registry.counter("05.fyi").inc())
    case _: PassThrough            => F.delay(registry.counter("06.pass.through").inc())
    case ActionStart(params, _, _) => F.delay(registry.counter(actionStartMRName(params.actionName)).inc())
    // timers
    case ActionFailed(params, info, at, _, _, _) =>
      F.delay(registry.timer(actionFailMRName(params.actionName)).update(Duration.between(info.launchTime, at)))

    case ActionRetrying(params, info, at, _, _) =>
      F.delay(registry.timer(actionRetryMRName(params.actionName)).update(Duration.between(info.launchTime, at)))

    case ActionQuasiSucced(params, info, at, _, _, _, _, _) =>
      F.delay(registry.timer(actionSuccMRName(params.actionName)).update(Duration.between(info.launchTime, at)))

    case ActionSucced(params, info, at, _, _) =>
      F.delay(registry.timer(actionSuccMRName(params.actionName)).update(Duration.between(info.launchTime, at)))

  }
}
