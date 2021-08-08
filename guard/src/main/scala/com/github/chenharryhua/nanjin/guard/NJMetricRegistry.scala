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
    // counter
    case _: ServiceStarted     => F.delay(registry.counter(s"service.start").inc())
    case _: ServicePanic       => F.delay(registry.counter(s"service.panic").inc())
    case _: ServiceStopped     => F.delay(registry.counter(s"service.stop").inc())
    case _: ServiceHealthCheck => F.delay(registry.counter(s"service.health.check").inc())
    case _: PassThrough        => F.delay(registry.counter("pass.through").inc())

    case ForYourInformation(_, _, isError) =>
      if (isError)
        F.delay(registry.counter("error.report").inc())
      else
        F.delay(registry.counter(s"fyi").inc())

    case ActionRetrying(_, info, _, _, _) => F.delay(registry.counter(s"action.[${info.actionName}].retry").inc())

    // timer
    case ActionFailed(at, info, _, _, _, _) =>
      F.delay(registry.timer(s"action.[${info.actionName}].fail").update(Duration.between(info.launchTime, at)))
    case ActionSucced(at, info, _, _, _) =>
      F.delay(registry.timer(s"action.[${info.actionName}].succ").update(Duration.between(info.launchTime, at)))
    case ActionQuasiSucced(at, info, _, _, _, _, _, _) =>
      F.delay(registry.timer(s"action.[${info.actionName}].quasi").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.delay(registry.removeMatching(MetricFilter.ALL))
    // no op
    case _: ActionStart => F.unit
  }
}
