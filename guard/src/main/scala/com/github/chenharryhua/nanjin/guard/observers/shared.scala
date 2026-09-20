package com.github.chenharryhua.nanjin.guard.observers

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import org.http4s.headers.`Idempotency-Key`

/** A stable idempotency key for `event`, used as the `Idempotency-Key` header when publishing to endpoints
  * that dedupe retries (see `SlackObserver`).
  *
  * The key is a deterministic function of the event's identity, so re-publishing the same event yields the
  * same key and the endpoint drops the duplicate. It is also distinct across the different events of a run:
  * each is scoped by `serviceId`, an event-kind tag, and a discriminator that is unique within that kind — a
  * tick index for start/panic/periodic-metrics, the scrape timestamp for adhoc metrics, the correlation id
  * for reported events. `ServiceStop` carries no discriminator: at most one stop is published per service.
  */
def idempotencyKey(event: Event): `Idempotency-Key` = event match {
  case Event.ServiceStart(serviceIdentity, _, _, tick) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-start-${tick.index}")
  case Event.ServicePanic(serviceIdentity, _, _, tick, _) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-panic-${tick.index}")
  case Event.ServiceStop(serviceIdentity, _, _, _, _) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-stop")

  case Event.MetricsSnapshot(serviceIdentity, _, MetricsSnapshot.Periodic(tick), _, _) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-metrics-periodic-${tick.index}")
  case Event.MetricsSnapshot(serviceIdentity, _, MetricsSnapshot.Adhoc(ts), _, _) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-metrics-adhoc-${ts.value.toInstant.toEpochMilli}")

  case Event.ReportedEvent(serviceIdentity, _, _, _, correlation, _, _, _) =>
    `Idempotency-Key`(show"${serviceIdentity.serviceId}-reported-$correlation")
}

/** Return a copy of `event` whose stack trace is truncated to the top `max` frames (the deepest, given
  * root-cause-first ordering); `None` leaves it unchanged. Only `ServicePanic` and `ReportedEvent` carry a
  * stack trace; every other event is returned as-is. Intended for the rendering path only: callers still emit
  * the original, untruncated event downstream.
  */
def limitStackTraceFrames(event: Event, max: Option[Int]): Event =
  event match {
    case p: Event.ServicePanic =>
      p.copy(stackTrace = max.fold(p.stackTrace)(p.stackTrace.topN))
    case r: Event.ReportedEvent =>
      r.copy(stackTrace = max.fold(r.stackTrace)(n => r.stackTrace.map(_.topN(n))))
    case other => other
  }
