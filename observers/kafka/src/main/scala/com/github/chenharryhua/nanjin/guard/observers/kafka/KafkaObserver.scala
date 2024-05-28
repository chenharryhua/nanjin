package com.github.chenharryhua.nanjin.guard.observers.kafka

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.generic.JsonCodec

import java.util.UUID

@JsonCodec
final case class NJEventKey(task: String, service: String)

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] =
    new KafkaObserver[F](ctx, Translator.idTranslator[F])
}

final class KafkaObserver[F[_]](ctx: KafkaContext[F], translator: Translator[F, NJEvent])(implicit
  F: Async[F])
    extends UpdateTranslator[F, NJEvent, KafkaObserver[F]] {

  def observe(topicName: TopicName): Pipe[F, NJEvent, NJEvent] = {
    def translate(evt: NJEvent): F[Option[ProducerRecord[KJson[NJEventKey], KJson[NJEvent]]]] =
      translator
        .translate(evt)
        .map(
          _.map(evt =>
            ProducerRecord(
              topicName.value,
              KJson(NJEventKey(evt.serviceParams.taskName.value, evt.serviceParams.serviceName.value)),
              KJson(evt))))

    (ss: Stream[F, NJEvent]) =>
      for {
        client <- ctx.topic[KJson[NJEventKey], KJson[NJEvent]](topicName).produce.client
        ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- ss
          .evalTap(ofm.monitoring)
          .evalTap(translate(_).flatMap(_.traverse(client.produceOne(_).flatten)))
          .onFinalize(ofm.terminated.flatMap(client.produce(_).flatten).void)
      } yield event
  }

  def observe(topicName: TopicNameL): Pipe[F, NJEvent, NJEvent] =
    observe(TopicName(topicName))

  override def updateTranslator(f: Endo[Translator[F, NJEvent]]): KafkaObserver[F] =
    new KafkaObserver(ctx, f(translator))
}
