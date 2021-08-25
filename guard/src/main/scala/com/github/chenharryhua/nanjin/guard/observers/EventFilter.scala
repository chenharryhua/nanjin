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
final private[observers] case class EventFilter(
  serviceStart: Boolean,
  servicePanic: Boolean,
  serviceStop: Boolean,
  actionSucc: Boolean,
  actionRetry: Boolean,
  actionFirstRetry: Boolean,
  actionStart: Boolean,
  actionFailure: Boolean,
  fyi: Boolean,
  passThrough: Boolean,
  mrReport: Boolean,
  sampling: Long
) extends Predicate[NJEvent] {

  override def apply(event: NJEvent): Boolean = event match {
    case _: ServiceStarted                 => serviceStart
    case _: ServicePanic                   => servicePanic
    case _: ServiceStopped                 => serviceStop
    case MetricsReport(idx, _, _, _, _, _) => mrReport && (0L === (idx % sampling))
    case _: ActionStart                    => actionStart
    case ActionRetrying(_, _, _, dr, _)    => actionRetry || (actionFirstRetry && dr.retriesSoFar === 0)
    case _: ActionFailed                   => actionFailure
    case _: ActionSucced                   => actionSucc
    case _: ActionQuasiSucced              => actionSucc
    case _: ForYourInformation             => fyi
    case _: PassThrough                    => passThrough
  }
}
