package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.{idempotencyKey, limitStackTraceFrames, FinalizeMonitor}
import com.github.chenharryhua.nanjin.guard.translator.*
import fs2.{Pipe, Stream}
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.`Idempotency-Key`
import org.http4s.{Method, Request, Uri}

/** Observer that renders each event as a Slack Block Kit message and POSTs it to a Slack incoming webhook.
  *
  * Obtain one with `SlackObserver.apply` and adjust its translator with `withTranslator` (each returns a new
  * observer). Wire it into a service with `observe`.
  */
sealed trait SlackObserver[F[_]] extends UpdateTranslator[F, SlackApp, SlackObserver[F]] {

  /** Transform the event-to-`SlackApp` translator, e.g. to skip certain event kinds. */
  override def withTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F]

  /** Truncate rendered stack traces to the top `num` frames (the deepest, root-cause-first). */
  def withMaxStackTraceFrames(num: Int): SlackObserver[F]

  /** Set the message icon URL, serialized as Slack's `icon_url` field. */
  def withIconUrl(link: Uri): SlackObserver[F]

  /** Build a pipe that observes events, renders each into a Slack message, and POSTs it to `webhook`.
    *
    * Events pass through unchanged (the pipe is a side-effecting tap). Each POST carries an idempotency key
    * so Slack can dedupe retries; a failed POST is swallowed so one bad publish does not tear down the
    * observer. On finalization any events the `FinalizeMonitor` still holds are flushed.
    *
    * @param webhook
    *   the Slack incoming-webhook URL to POST to.
    */
  def observe(webhook: Uri): Pipe[F, Event, Event]
}

object SlackObserver {
  def apply[F[_]: {Concurrent, Clock}](client: Resource[F, Client[F]]): SlackObserver[F] =
    new SlackObserverImpl[F](Params(client, SlackTranslator[F], None, None))
}

final private class SlackObserverImpl[F[_]: Clock](params: Params[F])(using F: Concurrent[F])
    extends SlackObserver[F] with Http4sClientDsl[F] {
  private def copy(p: Params[F]): SlackObserverImpl[F] = new SlackObserverImpl[F](p)

  override def withTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F] =
    copy(params.copy(translator = f(params.translator)))

  override def withMaxStackTraceFrames(num: Int): SlackObserver[F] =
    copy(params.copy(maxStackTraceFrames = Some(num)))

  override def withIconUrl(link: Uri): SlackObserver[F] =
    copy(params.copy(icon_url = Some(link)))

  private def publish(
    httpClient: Client[F],
    webhook: Uri,
    card: SlackApp,
    idempotencyKey: `Idempotency-Key`): F[Unit] = {
    val req = Request[F](method = Method.POST, uri = webhook)
      .withEntity(card)
      .withHeaders(idempotencyKey)
    httpClient.successful(req).attempt.void
  }

  // Translate one event, stamp the configured icon on the card, and POST it. Shared by the streaming tap and
  // the finalizer flush so the two paths cannot drift.
  private def publishEvent(httpClient: Client[F], webhook: Uri, evt: Event): F[Unit] =
    params.translator
      .translate(limitStackTraceFrames(evt, params.maxStackTraceFrames))
      .flatMap(_.traverse_ { card =>
        publish(
          httpClient,
          webhook,
          card.copy(icon_url = params.icon_url.map(_.renderString)),
          idempotencyKey(evt))
      })

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
