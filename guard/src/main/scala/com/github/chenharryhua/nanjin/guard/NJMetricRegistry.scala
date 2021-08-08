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
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}

import java.time.Duration

final private class NJMetricRegistry[F[_]](registry: MetricRegistry)(implicit F: Sync[F]) extends AlertService[F] {
  private def serviceKey(params: ServiceParams): String = s"[${params.serviceName}]-[${params.taskParams.appName}]"

  private def actionKey(info: ActionInfo, params: ActionParams): String =
    s"[${info.actionName}]-[${params.serviceParams.serviceName}]"

  override def alert(event: NJEvent): F[Unit] = event match {
    // counter
    case ServiceStarted(_, _, params) =>
      F.delay(registry.counter(s"service.${serviceKey(params)}.start").inc())
    case ServicePanic(_, _, params, _, _) =>
      F.delay(registry.counter(s"service.${serviceKey(params)}.panic").inc())
    case ServiceStopped(_, _, params) =>
      F.delay(registry.counter(s"service.${serviceKey(params)}.stop").inc())
    case ServiceHealthCheck(_, _, params, _) =>
      F.delay(registry.counter(s"service.${serviceKey(params)}.healthCheck").inc())
    case ActionRetrying(_, info, params, _, _) =>
      F.delay(registry.counter(s"action.${actionKey(info, params)}.retry").inc())
    case ForYourInformation(_, _, isError) =>
      if (isError)
        F.delay(registry.counter("error.report").inc())
      else
        F.delay(registry.counter(s"fyi").inc())
    case PassThrough(_, _) =>
      F.delay(registry.counter("pass-through").inc())
    // timer
    case ActionFailed(at, info, params, _, _, _) =>
      F.delay(registry.timer(s"action.${actionKey(info, params)}.fail").update(Duration.between(info.launchTime, at)))
    case ActionSucced(at, info, params, _, _) =>
      F.delay(registry.timer(s"action.${actionKey(info, params)}.succ").update(Duration.between(info.launchTime, at)))
    case ActionQuasiSucced(at, info, params, _, _, _, _, _) =>
      F.delay(registry.timer(s"action.${actionKey(info, params)}.quasi").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.delay(registry.removeMatching(MetricFilter.ALL))
    // no op
    case _: ActionStart => F.unit
  }
}
