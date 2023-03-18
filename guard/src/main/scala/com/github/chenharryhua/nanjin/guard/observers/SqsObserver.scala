package com.github.chenharryhua.nanjin.guard.observers

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleQueueService
import com.github.chenharryhua.nanjin.common.aws.SqsConfig
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import java.util.UUID

object SqsObserver {
  def apply[F[_]: Concurrent: Clock: UUIDGen](client: Resource[F, SimpleQueueService[F]]): SqsObserver[F] =
    new SqsObserver[F](client, Translator.idTranslator[F])
}

final class SqsObserver[F[_]: Clock: UUIDGen](
  client: Resource[F, SimpleQueueService[F]],
  translator: Translator[F, NJEvent])(implicit F: Concurrent[F])
    extends UpdateTranslator[F, NJEvent, SqsObserver[F]] {

  private def translate(evt: NJEvent): F[Option[Json]] =
    translator.translate(evt).flatMap(_.flatTraverse(Translator.verboseJson.translate))

  private def internal(builder: F[SendMessageRequest.Builder]): Pipe[F, NJEvent, NJEvent] =
    (es: Stream[F, NJEvent]) =>
      for {
        sqs <- Stream.resource(client)
        ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- es
          .evalTap(ofm.monitoring)
          .evalTap { e =>
            translate(e).flatMap(_.traverse(json =>
              builder.flatMap(b => sqs.sendMessage(b.messageBody(json.noSpaces).build()))))
          }
          .onFinalizeCase(ofm
            .terminated(_)
            .flatMap(_.traverse(json =>
              builder.flatMap(b => sqs.sendMessage(b.messageBody(json.noSpaces).build()))).void))
      } yield event

  def observe(builder: SendMessageRequest.Builder): Pipe[F, NJEvent, NJEvent] = internal(F.pure(builder))

  // events order should be preserved
  def observe(fifo: SqsConfig.Fifo): Pipe[F, NJEvent, NJEvent] = internal(
    UUIDGen
      .randomUUID[F]
      .map(uuid =>
        SendMessageRequest
          .builder()
          .messageDeduplicationId(uuid.show)
          .queueUrl(fifo.queueUrl)
          .messageGroupId(fifo.messageGroupId.value)))

  override def updateTranslator(f: Endo[Translator[F, NJEvent]]): SqsObserver[F] =
    new SqsObserver[F](client, f(translator))
}
