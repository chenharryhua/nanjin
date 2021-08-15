package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Async, Sync}
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.concurrent.Channel
import fs2.{INothing, Stream}

import java.time.Duration

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

final class ServiceGuard[F[_]] private[guard] (metricRegistry: MetricRegistry, serviceConfig: ServiceConfig)(implicit
  F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  val params: ServiceParams = serviceConfig.evalConfig

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, f(serviceConfig))

  def eventStream[A](actionGuard: ActionGuard[F] => F[A]): Stream[F, NJEvent] = {
    val scheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))
    val cron: CronExpr                    = Cron.unsafeParse(s"0 0 ${params.taskParams.metricsResetAt} ? * *")
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
                      error = NJError(ex, Importance.High)))
                  .void)
          ) {
            val startUp = realZonedDateTime(params).flatMap(ts =>
              channel.send(ServiceStarted(timestamp = ts, serviceInfo = si, serviceParams = params)))

            startUp *> Dispatcher[F].use(dispatcher =>
              actionGuard(
                new ActionGuard[F](
                  isFireStartEvent = false,
                  importance = Importance.Medium,
                  metricRegistry = metricRegistry,
                  serviceInfo = si,
                  dispatcher = dispatcher,
                  channel = channel,
                  actionName = "anonymous",
                  actionConfig = ActionConfig(params))))
          }
          .guarantee(realZonedDateTime(params).flatMap(ts =>
            channel.send(ServiceStopped(timestamp = ts, serviceInfo = si, serviceParams = params))) *> // stop event
            channel.close.void) // close channel and the stream as well

        /** concurrent streams
          */

        // fix-rate metrics report
        val reporting: Stream[F, INothing] =
          Stream
            .fixedRate[F](params.reportingInterval)
            .evalMap(_ =>
              realZonedDateTime(params)
                .flatMap(ts =>
                  channel.send(
                    MetricsReport(
                      timestamp = ts,
                      serviceInfo = si,
                      serviceParams = params,
                      metrics = MetricRegistryWrapper(Some(metricRegistry))
                    )))
                .void)
            .drain

        // reset metrics
        val metricsReset: Stream[F, INothing] =
          scheduler
            .awakeEvery(cron)
            .evalMap(_ =>
              for {
                ts <- realZonedDateTime(params)
                _ <- channel.send(
                  MetricsReset(
                    timestamp = ts,
                    serviceInfo = si,
                    serviceParams = params,
                    metrics = MetricRegistryWrapper(Some(metricRegistry))
                  ))
              } yield ())
            .drain

        // put together

        val accessories: Stream[F, INothing] =
          Stream(reporting, metricsReset, Stream.eval(theService).drain).parJoinUnbounded

        channel.stream.evalTap(mrService.compute[F]).concurrently(accessories)
      }
    } yield event
  }
}

final private class NJMetricRegistry(registry: MetricRegistry) {

  private def name(info: ActionInfo) = s"[${info.actionName}]"

  def compute[F[_]](event: NJEvent)(implicit F: Sync[F]): F[Unit] = event match {
    // counters
    case _: MetricsReport      => F.delay(registry.counter("01.health.check").inc())
    case _: ServiceStarted     => F.delay(registry.counter("02.service.start").inc())
    case _: ServiceStopped     => F.delay(registry.counter("03.service.stop").inc())
    case _: ServicePanic       => F.delay(registry.counter("04.service.`panic`").inc())
    case _: ActionStart        => F.delay(registry.counter(actionStartEventCountName).inc())
    case _: ForYourInformation => F.delay(registry.counter("06.fyi").inc())
    case _: PassThrough        => F.delay(registry.counter("07.pass.through").inc())

    // timers
    case ActionFailed(info, at, _, _, _, _) =>
      F.delay(registry.timer(s"08.`fail`.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionRetrying(info, at, _, _, _) =>
      F.delay(registry.timer(s"09.retry.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionQuasiSucced(info, at, _, _, _, _, _, _, _) =>
      F.delay(registry.timer(s"10.quasi.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionSucced(info, at, _, _, _, _) =>
      F.delay(registry.timer(s"11.succ.${name(info)}").update(Duration.between(info.launchTime, at)))

    // reset
    case _: MetricsReset => F.delay(registry.removeMatching(MetricFilter.ALL))
  }
}
