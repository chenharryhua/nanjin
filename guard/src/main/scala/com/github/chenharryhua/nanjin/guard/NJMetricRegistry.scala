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
  private def serviceKey(params: ServiceParams): String = s"${params.serviceName}.${params.taskParams.appName}"

  private def actionKey(info: ActionInfo, params: ActionParams): String =
    s"${info.actionName}.${params.serviceParams.serviceName}.${params.serviceParams.taskParams.appName}"

  override def alert(event: NJEvent): F[Unit] = event match {
    // counter
    case ServiceStarted(_, _, params) =>
      F.blocking(registry.counter(s"start.${serviceKey(params)}").inc())
    case ServicePanic(_, _, params, _, _) =>
      F.blocking(registry.counter(s"panic.${serviceKey(params)}").inc())
    case ServiceStopped(_, _, params) =>
      F.blocking(registry.counter(s"stop.${serviceKey(params)}").inc())
    case ServiceHealthCheck(_, _, params, _, _, _) =>
      F.blocking(registry.counter(s"health-check.${serviceKey(params)}").inc())
    case ActionRetrying(_, info, params, _, _) =>
      F.blocking(registry.counter(s"retry.${actionKey(info, params)}").inc())
    case ForYourInformation(_, _, isError) =>
      if (isError)
        F.blocking(registry.counter("report.error").inc())
      else
        F.blocking(registry.counter(s"fyi").inc())
    case PassThrough(_, _) =>
      F.blocking(registry.counter("pass-through").inc())
    // timer
    case ActionFailed(at, info, params, _, _, _) =>
      F.blocking(registry.timer(s"fail.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))
    case ActionSucced(at, info, params, _, _) =>
      F.blocking(registry.timer(s"succ.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))
    case ActionQuasiSucced(at, info, params, _, _, _, _, _) =>
      F.blocking(registry.timer(s"quasi.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.blocking(registry.removeMatching(MetricFilter.ALL))
    // no op
    case _: ActionStart => F.unit
  }
}
