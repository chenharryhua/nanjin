package com.github.chenharryhua.nanjin.guard.observers

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot

def idempotencyKey(event: Event): String = event match {
  case Event.ServiceStart(serviceIdentity, _, _, tick) =>
    show"${serviceIdentity.serviceId}-start-${tick.index}"
  case Event.ServicePanic(serviceIdentity, _, _, tick, _) =>
    show"${serviceIdentity.serviceId}-panic-${tick.index}"
  case Event.ServiceStop(serviceIdentity, _, _, _, _) =>
    show"${serviceIdentity.serviceId}-stop"

  case Event.MetricsSnapshot(serviceIdentity, _, MetricsSnapshot.Periodic(tick), _, _) =>
    show"${serviceIdentity.serviceId}-metrics-periodic-${tick.index}"
  case Event.MetricsSnapshot(serviceIdentity, _, MetricsSnapshot.Adhoc(ts), _, _) =>
    show"${serviceIdentity.serviceId}-metrics-adhoc-${ts.value.toInstant.toEpochMilli}"

  case Event.ReportedEvent(serviceIdentity, _, _, _, correlation, _, _, _) =>
    show"${serviceIdentity.serviceId}-reported-$correlation"
}
