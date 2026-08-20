package com.github.chenharryhua.nanjin.guard.observers.splunk

import cats.Endo
import cats.effect.kernel.{Concurrent, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{PrettyJsonTranslator, Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Content-Type`, Authorization}
import org.http4s.{AuthScheme, Credentials, MediaType, Uri}

/** Splunk HTTP Event Collector (HEC) configuration.
  *
  * @param endpoint
  *   the HEC endpoint URI, e.g. `https://splunk.example.com:8088/services/collector/event`
  * @param token
  *   the HEC authentication token
  * @param index
  *   optional Splunk index (defaults to the token's configured index if absent)
  * @param source
  *   optional source metadata
  * @param sourceType
  *   optional source type (defaults to `_json` if absent)
  */
final case class HecConfig(
  endpoint: Uri,
  token: String,
  index: Option[String] = None,
  source: Option[String] = None,
  sourceType: Option[String] = None
)

object SplunkObserver {
  def apply[F[_]: Concurrent](client: Resource[F, Client[F]]): SplunkObserver[F] =
    new SplunkObserver[F](client, PrettyJsonTranslator[F])
}

/** Observes service events and posts them to Splunk via HTTP Event Collector.
  *
  * Each event is wrapped in the HEC JSON envelope:
  * {{{
  * {
  *   "time": <epoch_seconds>,
  *   "event": { ... translated JSON ... },
  *   "sourcetype": "_json",
  *   "index": "main",
  *   "source": "nanjin"
  * }
  * }}}
  *
  * Usage:
  * {{{
  *   val config = HecConfig(
  *     endpoint = uri"https://splunk.example.com:8088/services/collector/event",
  *     token = "your-hec-token"
  *   )
  *   val observer = SplunkObserver[IO](httpClientResource)
  *   eventStream.through(observer.observe(config))
  * }}}
  */
final class SplunkObserver[F[_]](client: Resource[F, Client[F]], translator: Translator[F, Json])(using
  F: Concurrent[F])
    extends UpdateTranslator[F, Json, SplunkObserver[F]] with Http4sClientDsl[F] {

  override def updateTranslator(f: Endo[Translator[F, Json]]): SplunkObserver[F] =
    new SplunkObserver[F](client, f(translator))

  private def buildEnvelope(event: Event, json: Json, config: HecConfig): Json = {
    val epoch: Double = event.timestamp.value.toInstant.toEpochMilli / 1000.0
    val fields = List(
      "time" -> Json.fromDoubleOrNull(epoch),
      "event" -> json,
      "sourcetype" -> Json.fromString(config.sourceType.getOrElse("_json"))
    ) ++
      config.index.map("index" -> Json.fromString(_)) ++
      config.source.map("source" -> Json.fromString(_))

    Json.obj(fields*)
  }

  private def publish(httpClient: Client[F], config: HecConfig, payload: Json): F[Unit] = {
    val req = POST(payload.noSpaces, config.endpoint)
      .withContentType(`Content-Type`(MediaType.application.json))
      .putHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, config.token)))
    httpClient.successful(req).attempt.void
  }

  def observe(config: HecConfig): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      http <- Stream.resource(client)
      event <- es.evalTap { e =>
        translator.translate(e).flatMap(_.traverse { json =>
          publish(http, config, buildEnvelope(e, json, config))
        }).void
      }
    } yield event
}
