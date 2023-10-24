package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.generic.JsonCodec

@JsonCodec
final case class NJEventKey(task: String, service: String)

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] = new KafkaObserver[F](ctx)
}

final class KafkaObserver[F[_]: Async](ctx: KafkaContext[F]) {

  def observe(topicName: TopicName): Pipe[F, NJEvent, Nothing] = { (ss: Stream[F, NJEvent]) =>
    ss.map(evt =>
      ProducerRecord(
        topicName.value,
        KJson(NJEventKey(evt.serviceParams.taskParams.taskName, evt.serviceParams.serviceName)),
        KJson(evt)))
      .chunks
      .through(ctx.topic[KJson[NJEventKey], KJson[NJEvent]](topicName).produce.pipe)
      .drain
  }

  def observe(topicName: TopicNameL): Pipe[F, NJEvent, Nothing] =
    observe(TopicName(topicName))
}
