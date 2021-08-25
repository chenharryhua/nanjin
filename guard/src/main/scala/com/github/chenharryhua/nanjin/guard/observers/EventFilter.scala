package com.github.chenharryhua.nanjin.guard.observers

import cats.collections.Predicate
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.guard.event.{
  ActionFailed,
  ActionQuasiSucced,
  ActionRetrying,
  ActionStart,
  ActionSucced,
  ForYourInformation,
  MetricsReport,
  NJEvent,
  PassThrough,
  ServicePanic,
  ServiceStarted,
  ServiceStopped
}
import monocle.macros.Lenses

@Lenses final case class EventFilter(
  serviceStarted: Boolean,
  servicePanic: Boolean,
  serviceStopped: Boolean,
  actionSucced: Boolean,
  actionRetrying: Boolean,
  actionFirstRetry: Boolean,
  actionStart: Boolean,
  actionFailed: Boolean,
  fyi: Boolean,
  passThrough: Boolean,
  metricsReport: Boolean,
  sampling: Long
) extends Predicate[NJEvent] {

  override def apply(event: NJEvent): Boolean = event match {
    case _: ServiceStarted                 => serviceStarted
    case _: ServicePanic                   => servicePanic
    case _: ServiceStopped                 => serviceStopped
    case MetricsReport(idx, _, _, _, _, _) => metricsReport && (0L === (idx % sampling))
    case _: ActionStart                    => actionStart
    case ActionRetrying(_, _, _, dr, _)    => actionRetrying || (actionFirstRetry && dr.retriesSoFar === 0)
    case _: ActionFailed                   => actionFailed
    case _: ActionSucced                   => actionSucced
    case _: ActionQuasiSucced              => actionSucced
    case _: ForYourInformation             => fyi
    case _: PassThrough                    => passThrough
  }
}

object EventFilter {
  val all: EventFilter = EventFilter(
    serviceStarted = true,
    servicePanic = true,
    serviceStopped = true,
    actionSucced = true,
    actionRetrying = true,
    actionFirstRetry = true,
    actionStart = true,
    actionFailed = true,
    fyi = true,
    passThrough = true,
    metricsReport = true,
    sampling = 1
  )
  val none: EventFilter = EventFilter(
    serviceStarted = false,
    servicePanic = false,
    serviceStopped = false,
    actionSucced = false,
    actionRetrying = false,
    actionFirstRetry = false,
    actionStart = false,
    actionFailed = false,
    fyi = false,
    passThrough = false,
    metricsReport = false,
    sampling = 1
  )
}
