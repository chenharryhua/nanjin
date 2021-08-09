package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
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

  override def alert(event: NJEvent): F[Unit] = event match {
    case _: ServiceHealthCheck                        => F.delay(registry.counter("001.health.check").inc())
    case _: ServiceStarted                            => F.delay(registry.counter("002.service.start").inc())
    case _: ServiceStopped                            => F.delay(registry.counter("003.service.stop").inc())
    case _: ServicePanic                              => F.delay(registry.counter("`004.service.panic`").inc())
    case ForYourInformation(_, _, isError) if isError => F.delay(registry.counter("`005.error.report`").inc())
    case _: ForYourInformation                        => F.delay(registry.counter("006.fyi").inc())
    case _: PassThrough                               => F.delay(registry.counter("007.pass.through").inc())

    case ActionFailed(at, info, _, _, _, _) =>
      F.delay(registry.timer(s"`008.fail.[${info.actionName}]`").update(Duration.between(info.launchTime, at)))
    case ActionRetrying(_, info, _, _, _) => F.delay(registry.counter(s"`009.retry.[${info.actionName}]`").inc())

    case ActionQuasiSucced(at, info, _, _, _, _, _, _) =>
      F.delay(registry.timer(s"010.quasi.[${info.actionName}]").update(Duration.between(info.launchTime, at)))

    case ActionSucced(at, info, _, _, _) =>
      F.delay(registry.timer(s"011.succ[${info.actionName}]").update(Duration.between(info.launchTime, at)))
    // reset
    case _: ServiceDailySummariesReset => F.delay(registry.removeMatching(MetricFilter.ALL))
    // no op
    case _: ActionStart => F.unit
  }
}
