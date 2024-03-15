package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.Translator
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.generic.JsonCodec

import java.util.UUID

@JsonCodec
final case class NJEventKey(task: String, service: String)

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] = new KafkaObserver[F](ctx)
}

final class KafkaObserver[F[_]](ctx: KafkaContext[F])(implicit F: Async[F]) {

  def observe(topicName: TopicName): Pipe[F, NJEvent, NJEvent] = { (ss: Stream[F, NJEvent]) =>
    for {
      client <- ctx.topic[KJson[NJEventKey], KJson[NJEvent]](topicName).produce.client
      ofm <- Stream.eval(
        F.ref[Map[UUID, ServiceStart]](Map.empty)
          .map(new FinalizeMonitor(Translator.idTranslator.translate, _)))
      event <- ss
        .evalTap(ofm.monitoring)
        .evalTap { evt =>
          val msg = ProducerRecord(
            topicName.value,
            KJson(NJEventKey(evt.serviceParams.taskParams.taskName, evt.serviceParams.serviceName)),
            KJson(evt))
          client.produceOne(msg).flatten
        }
        .onFinalize(
          ofm.terminated
            .map(
              _.map(evt =>
                ProducerRecord(
                  topicName.value,
                  KJson(NJEventKey(evt.serviceParams.taskParams.taskName, evt.serviceParams.serviceName)),
                  KJson(evt))))
            .flatMap(client.produce(_).flatten)
            .void)
    } yield event
  }

  def observe(topicName: TopicNameL): Pipe[F, NJEvent, NJEvent] =
    observe(TopicName(topicName))
}
