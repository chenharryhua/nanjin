package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Method, Request, Uri}

object TeamsObserver {
  def apply[F[_]: {Concurrent, Clock}](client: Resource[F, Client[F]]): TeamsObserver[F] =
    new TeamsObserver[F](client, TeamsTranslator[F])
}

/** Observes service events and posts them to a Microsoft Teams channel via incoming webhook.
  *
  * Usage:
  * {{{
  *   val webhook: Uri = uri"https://outlook.office.com/webhook/..."
  *   val observer = TeamsObserver[IO](httpClientResource)
  *   eventStream.through(observer.observe(webhook))
  * }}}
  */
final class TeamsObserver[F[_]: Clock] private (
  client: Resource[F, Client[F]],
  translator: Translator[F, AdaptiveCard])(using F: Concurrent[F])
    extends UpdateTranslator[F, AdaptiveCard, TeamsObserver[F]] with Http4sClientDsl[F] {

  override def withTranslator(f: Endo[Translator[F, AdaptiveCard]]): TeamsObserver[F] =
    new TeamsObserver[F](client, f(translator))

  // No `Idempotency-Key` header: Teams' incoming webhook accepts the POST (HTTP 2xx) but then silently fails
  // to render the card when that header is present; omitting it makes the card render. Teams webhooks do not
  // honour `Idempotency-Key` for dedup anyway, so nothing is lost. This is a deliberate divergence from the
  // Slack observer, whose endpoint renders fine with the header and keeps it for retry dedup.
  private def publish(httpClient: Client[F], webhook: Uri, card: AdaptiveCard): F[Unit] = {
    val req = Request[F](method = Method.POST, uri = webhook).withEntity(card)
    httpClient.successful(req).attempt.void
  }

  def observe(webhook: Uri): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      http <- Stream.resource(client)
      ofm <- Stream.eval(FinalizeMonitor[F])
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap { e =>
          translator.translate(e)
            .flatMap(_.traverse(card => publish(http, webhook, card)))
        }
        .onFinalize {
          ofm.terminated
            .flatMap(_.traverse_ { e =>
              translator
                .translate(e)
                .flatMap(_.traverse_(card => publish(http, webhook, card)))
            })
        }
    } yield event
}
