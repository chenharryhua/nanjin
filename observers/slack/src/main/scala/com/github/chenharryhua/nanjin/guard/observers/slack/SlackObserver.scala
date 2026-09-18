package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.{idempotencyKey, FinalizeMonitor}
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
    new SlackObserverImpl[F](client, SlackTranslator[F])
}

final private class SlackObserverImpl[F[_]: Clock](
  client: Resource[F, Client[F]],
  translator: Translator[F, SlackApp])(using F: Concurrent[F])
    extends SlackObserver[F] with Http4sClientDsl[F] {

  override def withTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F] =
    new SlackObserverImpl[F](client, f(translator))

  private def publish(
    httpClient: Client[F],
    webhook: Uri,
    card: SlackApp,
    idempotencyKey: String): F[Unit] = {
    val req = Request[F](method = Method.POST, uri = webhook)
      .withEntity(card)
      .withHeaders(`Idempotency-Key`(idempotencyKey))
    httpClient.successful(req).attempt.void
  }

  override def observe(webhook: Uri): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      http <- Stream.resource(client)
      ofm <- Stream.eval(FinalizeMonitor[F])
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(e =>
          translator
            .translate(e)
            .flatMap(_.traverse(card => publish(http, webhook, card, idempotencyKey(e)))))
        .onFinalize(ofm.terminated
          .flatMap(_.traverse_ { e =>
            translator.translate(e)
              .flatMap(_.traverse_(card => publish(http, webhook, card, idempotencyKey(e))))
          }))
    } yield event
}
