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
    case _: ServiceHealthCheck                        => F.delay(registry.counter("01.health.check").inc())
    case _: ServiceStarted                            => F.delay(registry.counter("02.service.start").inc())
    case _: ServiceStopped                            => F.delay(registry.counter("03.service.stop").inc())
    case _: ServicePanic                              => F.delay(registry.counter("04.`service.panic`").inc())
    case ForYourInformation(_, _, isError) if isError => F.delay(registry.counter("05.`error.report`").inc())
    case _: ForYourInformation                        => F.delay(registry.counter("06.fyi").inc())
    case _: PassThrough                               => F.delay(registry.counter("07.pass.through").inc())
    case _: ActionStart                               => F.delay(registry.counter("08.action.count").inc())

    case ActionFailed(at, severity, info, _, _, _, err) =>
      F.delay(
        registry
          .timer(s"1${severity.value}.`${severity.entryName}`.${name(info)}")
          .update(Duration.between(info.launchTime, at)))

    case ActionRetrying(_, severity, info, _, _, _) =>
      F.delay(registry.counter(s"2${severity.value}.retry.${severity.entryName}.${name(info)}").inc())

    case ActionQuasiSucced(at, _, info, _, _, _, _, _, _) =>
      F.delay(registry.timer(s"30.quasi.${name(info)}").update(Duration.between(info.launchTime, at)))

    case ActionSucced(at, _, info, _, _, _) =>
      F.delay(registry.timer(s"31.succ.${name(info)}").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.delay(registry.removeMatching(MetricFilter.ALL))
    // no op
  }
}
