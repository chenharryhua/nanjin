package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Temporal}
import cats.syntax.all.*
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}
import com.github.chenharryhua.nanjin.aws.SimpleQueueService
import com.github.chenharryhua.nanjin.common.aws.SqsUrl
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json

import java.util.UUID
object SqsObserver {
  def apply[F[_]: Temporal](client: Resource[F, SimpleQueueService[F]]) =
    new SqsObserver[F](client, Translator.verboseJson[F])
}
final class SqsObserver[F[_]](client: Resource[F, SimpleQueueService[F]], translator: Translator[F, Json])(
  implicit F: Temporal[F])
    extends UpdateTranslator[F, Json, SqsObserver[F]] {

  private def sendMessage(
    sqs: SimpleQueueService[F],
    sqsUrl: SqsUrl,
    js: Json): F[Either[Throwable, SendMessageResult]] = {
    val req = new SendMessageRequest(sqsUrl.value, js.noSpaces)
    sqs.sendMessage(req).attempt
  }

  def observe(sqsUrl: SqsUrl): Pipe[F, NJEvent, NJEvent] = (es: Stream[F, NJEvent]) =>
    for {
      sqs <- Stream.resource(client)
      ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator, _)))
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(e => translator.translate(e).flatMap(_.traverse(sendMessage(sqs, sqsUrl, _))).void)
        .onFinalizeCase(ofm.terminated(_).flatMap(_.traverse(sendMessage(sqs, sqsUrl, _))).void)
    } yield event

  override def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): SqsObserver[F] =
    new SqsObserver[F](client, f(translator))
}
