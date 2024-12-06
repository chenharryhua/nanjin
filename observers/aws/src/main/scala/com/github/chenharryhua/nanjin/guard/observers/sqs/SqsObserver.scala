package com.github.chenharryhua.nanjin.guard.observers.sqs

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleQueueService
import com.github.chenharryhua.nanjin.common.aws.SqsUrl
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.sqs.model.{SendMessageRequest, SendMessageResponse}

import java.util.UUID

object SqsObserver {
  def apply[F[_]: Concurrent: Clock: UUIDGen](client: Resource[F, SimpleQueueService[F]]): SqsObserver[F] =
    new SqsObserver[F](client, Translator.idTranslator[F])
}

final class SqsObserver[F[_]: Clock: UUIDGen](
  client: Resource[F, SimpleQueueService[F]],
  translator: Translator[F, Event])(implicit F: Concurrent[F])
    extends UpdateTranslator[F, Event, SqsObserver[F]] {

  private def translate(evt: Event): F[Option[Json]] =
    translator.translate(evt).map(_.map(_.asJson))

  private def send(
    sqs: SimpleQueueService[F],
    builder: SendMessageRequest.Builder,
    json: Json): F[Either[Throwable, SendMessageResponse]] =
    UUIDGen[F].randomUUID.flatMap(uuid =>
      sqs.sendMessage(builder.messageBody(json.noSpaces).messageDeduplicationId(uuid.show).build()).attempt)

  private def internal(builder: SendMessageRequest.Builder): Pipe[F, Event, Event] =
    (es: Stream[F, Event]) =>
      for {
        sqs <- Stream.resource(client)
        ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- es
          .evalTap(ofm.monitoring)
          .evalTap { e =>
            translate(e).flatMap(_.traverse(json => send(sqs, builder, json)))
          }
          .onFinalize(ofm.terminated.flatMap(_.traverse(json => send(sqs, builder, json))).void)
      } yield event

  def observe(builder: SendMessageRequest.Builder): Pipe[F, Event, Event] = internal(builder)

  // events order should be preserved
  def observe(url: SqsUrl.Fifo, messageGroupId: String): Pipe[F, Event, Event] =
    internal(SendMessageRequest.builder().queueUrl(url.value).messageGroupId(messageGroupId))

  override def updateTranslator(f: Endo[Translator[F, Event]]): SqsObserver[F] =
    new SqsObserver[F](client, f(translator))
}
