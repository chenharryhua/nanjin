package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import cats.Endo
import cats.effect.std.UUIDGen
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}
import com.github.chenharryhua.nanjin.aws.SimpleQueueService
import com.github.chenharryhua.nanjin.common.aws.SqsConfig
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json

import java.util.UUID
object SqsObserver {
  def apply[F[_]: Async](client: Resource[F, SimpleQueueService[F]]) =
    new SqsObserver[F](client, Translator.idTranslator[F])
}
final class SqsObserver[F[_]](client: Resource[F, SimpleQueueService[F]], translator: Translator[F, NJEvent])(
  implicit F: Async[F])
    extends UpdateTranslator[F, NJEvent, SqsObserver[F]] {
  private def translate(evt: NJEvent): F[Option[Json]] =
    translator.translate(evt).flatMap(_.flatTraverse(Translator.verboseJson.translate))
  private def sendMessage(
    sqs: SimpleQueueService[F],
    f: SendMessageRequest => F[SendMessageRequest],
    js: Json): F[Either[Throwable, SendMessageResult]] =
    f(new SendMessageRequest()).map(_.withMessageBody(js.noSpaces)).flatMap(sqs.sendMessage).attempt

  private def internal(f: SendMessageRequest => F[SendMessageRequest]): Pipe[F, NJEvent, NJEvent] =
    (es: Stream[F, NJEvent]) =>
      for {
        sqs <- Stream.resource(client)
        ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- es
          .evalTap(ofm.monitoring)
          .evalTap(e => translate(e).flatMap(_.traverse(sendMessage(sqs, f, _))).void)
          .onFinalizeCase(ofm.terminated(_).flatMap(_.traverse(sendMessage(sqs, f, _))).void)
      } yield event

  def observe(f: Endo[SendMessageRequest]): Pipe[F, NJEvent, NJEvent] = internal(m => F.pure(f(m)))

  def observe(fifo: SqsConfig.Fifo): Pipe[F, NJEvent, NJEvent] = internal((req: SendMessageRequest) =>
    UUIDGen
      .randomUUID[F]
      .map(uuid =>
        req
          .withMessageDeduplicationId(uuid.show)
          .withQueueUrl(fifo.queueUrl)
          .withMessageGroupId(fifo.messageGroupId.value)))

  override def updateTranslator(f: Translator[F, NJEvent] => Translator[F, NJEvent]): SqsObserver[F] =
    new SqsObserver[F](client, f(translator))
}
