package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.{limitStackTraceFrames, FinalizeMonitor}
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Method, Request, Uri}

/** Observes service events and posts them to a Microsoft Teams channel via incoming webhook.
  *
  * Obtain one with `TeamsObserver.apply` and adjust its translator with `withTranslator` (each returns a new
  * observer). Wire it into a service with `observe`.
  *
  * Usage:
  * {{{
  *   val webhook: Uri = uri"https://outlook.office.com/webhook/..."
  *   val observer = TeamsObserver[IO](httpClientResource)
  *   eventStream.through(observer.observe(webhook))
  * }}}
  */
sealed trait TeamsObserver[F[_]] extends UpdateTranslator[F, AdaptiveCard, TeamsObserver[F]] {

  /** Transform the event-to-`AdaptiveCard` translator, e.g. to skip certain event kinds. */
  override def withTranslator(f: Endo[Translator[F, AdaptiveCard]]): TeamsObserver[F]

  def withMaxStackTraceFrames(num: Int): TeamsObserver[F]

  /** Build a pipe that observes events, renders each into an `AdaptiveCard`, and POSTs it to `webhook`.
    *
    * Events pass through unchanged (the pipe is a side-effecting tap). A failed POST is swallowed so one bad
    * publish does not tear down the observer. On finalization any events the `FinalizeMonitor` still holds
    * are flushed.
    *
    * @param webhook
    *   the Teams incoming-webhook URL to POST to.
    */
  def observe(webhook: Uri): Pipe[F, Event, Event]
}

object TeamsObserver {
  def apply[F[_]: {Concurrent, Clock}](client: Resource[F, Client[F]]): TeamsObserver[F] =
    new TeamsObserverImpl[F](Params(client, TeamsTranslator[F], None))
}

final private class TeamsObserverImpl[F[_]: Clock](params: Params[F])(using F: Concurrent[F])
    extends TeamsObserver[F] with Http4sClientDsl[F] {
  private def copy(p: Params[F]): TeamsObserverImpl[F] =
    new TeamsObserverImpl[F](p)

  override def withTranslator(f: Endo[Translator[F, AdaptiveCard]]): TeamsObserver[F] =
    copy(params.copy(translator = f(params.translator)))

  override def withMaxStackTraceFrames(num: Int): TeamsObserver[F] =
    copy(params.copy(maxStackTraceFrames = Some(num)))

  // No `Idempotency-Key` header: Teams' incoming webhook accepts the POST (HTTP 2xx) but then silently fails
  // to render the card when that header is present; omitting it makes the card render. Teams webhooks do not
  // honour `Idempotency-Key` for dedup anyway, so nothing is lost. This is a deliberate divergence from the
  // Slack observer, whose endpoint renders fine with the header and keeps it for retry dedup.
  private def publishEvent(httpClient: Client[F], webhook: Uri, event: Event): F[Unit] =
    params.translator.translate(limitStackTraceFrames(event, params.maxStackTraceFrames))
      .flatMap(_.traverse { card =>
        val req = Request[F](method = Method.POST, uri = webhook).withEntity(card)
        httpClient.successful(req).attempt
      }).void

  override def observe(webhook: Uri): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      http <- Stream.resource(params.client)
      ofm <- Stream.eval(FinalizeMonitor[F])
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(publishEvent(http, webhook, _))
        .onFinalize(ofm.terminated.flatMap(_.traverse_(publishEvent(http, webhook, _))))
    } yield event
}
