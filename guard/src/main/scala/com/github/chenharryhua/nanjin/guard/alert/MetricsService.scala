package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.Sync
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}

import java.time.Duration

final private class MetricsService[F[_]](metrics: MetricRegistry)(implicit F: Sync[F]) extends AlertService[F] {
  private def serviceKey(params: ServiceParams): String = s"${params.serviceName}.${params.taskParams.appName}"

  private def actionKey(info: ActionInfo, params: ActionParams): String =
    s"${info.actionName}.${params.serviceParams.serviceName}.${params.serviceParams.taskParams.appName}"

  override def alert(event: NJEvent): F[Unit] = event match {
    // counter
    case ServiceStarted(_, _, params) =>
      F.blocking(metrics.counter(s"start.${serviceKey(params)}").inc())
    case ServicePanic(_, _, params, _, _) =>
      F.blocking(metrics.counter(s"panic.${serviceKey(params)}").inc())
    case ServiceStopped(_, _, params) =>
      F.blocking(metrics.counter(s"stop.${serviceKey(params)}").inc())
    case ServiceHealthCheck(_, _, params, _, _, _) =>
      F.blocking(metrics.counter(s"health-check.${serviceKey(params)}").inc())
    case ActionRetrying(_, info, params, _, _) =>
      F.blocking(metrics.counter(s"retry.${actionKey(info, params)}").inc())
    case ForYourInformation(_, _, isError) =>
      if (isError)
        F.blocking(metrics.counter("report.error").inc())
      else
        F.blocking(metrics.counter(s"fyi").inc())
    case PassThrough(_, _) =>
      F.blocking(metrics.counter("pass-through").inc())
    // timer
    case ActionFailed(at, info, params, _, _, _) =>
      F.blocking(metrics.timer(s"fail.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))
    case ActionSucced(at, info, params, _, _) =>
      F.blocking(metrics.timer(s"succ.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))
    case ActionQuasiSucced(at, info, params, _, _, _, _, _) =>
      F.blocking(metrics.timer(s"quasi.${actionKey(info, params)}").update(Duration.between(info.launchTime, at)))

    // reset
    case _: ServiceDailySummariesReset => F.blocking(metrics.removeMatching(MetricFilter.ALL))
    // no op
    case _: ActionStart => F.unit
  }
}
