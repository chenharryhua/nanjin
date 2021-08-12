package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionInfo,
  ActionQuasiSucced,
  ActionRetrying,
  ActionStart,
  ActionSucced,
  AlertService,
  ForYourInformation,
  NJEvent,
  PassThrough,
  ServiceDailySummariesReset,
  ServiceHealthCheck,
  ServicePanic,
  ServiceStarted,
  ServiceStopped
}

import java.time.Duration

final private class NJMetricRegistry[F[_]](registry: MetricRegistry)(implicit F: Sync[F]) extends AlertService[F] {

  private def name(info: ActionInfo) = s"[${info.actionName}]"

  override def alert(event: NJEvent): F[Unit] = event match {
    // counters
    case _: ServiceHealthCheck => F.delay(registry.counter("01.health.check").inc())
    case _: ServiceStarted     => F.delay(registry.counter("02.service.start").inc())
    case _: ServiceStopped     => F.delay(registry.counter("03.service.stop").inc())
    case _: ServicePanic       => F.delay(registry.counter("04.`service.panic`").inc())
    case _: ForYourInformation => F.delay(registry.counter("05.fyi").inc())
    case _: PassThrough        => F.delay(registry.counter("06.pass.through").inc())
    case _: ActionStart        => F.delay(registry.counter("07.action.count").inc())

    // timers
    case ActionFailed(at, info, _, _, _, err) =>
      F.delay(
        registry
          .timer(s"1${err.severity.value}.`${err.severity.entryName}`.${name(info)}")
          .update(Duration.between(info.launchTime, at)))

    case ActionRetrying(at, info, _, _, _) =>
      F.delay(registry.timer(s"20.retry.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionQuasiSucced(at, _, info, _, _, _, _, _, _) =>
      F.delay(registry.timer(s"21.quasi.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionSucced(at, _, info, _, _, _) =>
      F.delay(registry.timer(s"22.succ.${name(info)}").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.delay(registry.removeMatching(MetricFilter.ALL))
    // no op
  }
}
