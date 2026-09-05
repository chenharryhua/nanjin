package com.github.chenharryhua.nanjin.guard.observers.sqs

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.show.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.aws.SimpleQueueService
import com.github.chenharryhua.nanjin.aws.SqsUrl
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import com.github.chenharryhua.nanjin.guard.config.ServiceId

object SqsObserver {

  /** Create an observer that forwards events to an SQS queue, using the identity translator (each event is
    * sent verbatim as JSON). Refine the translator with `withTranslator`.
    */
  def apply[F[_]: {Concurrent, Clock, UUIDGen}](client: Resource[F, SimpleQueueService[F]]): SqsObserver[F] =
    new SqsObserver[F](client, Translator.idTranslator[F])
}

/** Observer that sends each event to an AWS SQS queue as a JSON message.
  *
  * Every event is translated and, if the translator keeps it, published immediately (no batching). On stream
  * finalization, a `ServiceStop` is synthesized and sent for each service still running, so a cancelled or
  * crashed service is still reported. Send failures are swallowed (see `send`) so one failed publish does not
  * tear down the observer.
  */
final class SqsObserver[F[_]: {Clock, UUIDGen}](
  client: Resource[F, SimpleQueueService[F]],
  translator: Translator[F, Event])(using F: Concurrent[F])
    extends UpdateTranslator[F, Event, SqsObserver[F]] {

  private def translate(evt: Event): F[Option[Json]] =
    translator.translate(evt).map(_.map(_.asJson))

  // Send one message, tagging it with a random deduplication id. attempt swallows failures so a single
  // failed publish does not terminate the observer stream.
  private def send(sqs: SimpleQueueService[F], builder: SendMessageRequest.Builder, json: Json): F[Unit] =
    UUIDGen[F].randomUUID.flatMap(uuid =>
      sqs
        .send(builder.messageBody(json.noSpaces).messageDeduplicationId(uuid.show).build())
        .attempt
        .void)

  private def internal(builder: SendMessageRequest.Builder): Pipe[F, Event, Event] =
    (es: Stream[F, Event]) =>
      for {
        sqs <- Stream.resource(client)
        ofm <- Stream.eval(
          F.ref[Map[ServiceId, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- es
          .evalTap(ofm.monitoring)
          .evalTap(e => translate(e).flatMap(_.traverse(json => send(sqs, builder, json))))
          .onFinalize(ofm.terminated.flatMap(_.traverse_(json => send(sqs, builder, json))))
      } yield event

  /** Observe events, sending each to the queue configured by `builder`. Events pass through unchanged.
    *
    * @param builder
    *   a partially built `SendMessageRequest` (e.g. with the queue URL set); the message body and
    *   deduplication id are filled in per event.
    */
  def observe(builder: SendMessageRequest.Builder): Pipe[F, Event, Event] = internal(builder)

  /** Observe events, sending each to a FIFO queue under `messageGroupId`.
    *
    * A single message group preserves event ordering, since FIFO queues order messages within a group.
    *
    * @param url
    *   the FIFO queue URL.
    * @param messageGroupId
    *   the FIFO message group; all events share it to keep their order.
    */
  def observe(url: SqsUrl.Fifo, messageGroupId: String): Pipe[F, Event, Event] =
    internal(SendMessageRequest.builder().queueUrl(url.value).messageGroupId(messageGroupId))

  /** Transform the event translator, e.g. to filter or reshape the JSON sent to SQS. */
  override def withTranslator(f: Endo[Translator[F, Event]]): SqsObserver[F] =
    new SqsObserver[F](client, f(translator))
}
