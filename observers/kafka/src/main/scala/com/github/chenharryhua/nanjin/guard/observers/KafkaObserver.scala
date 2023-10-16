package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] = new KafkaObserver[F](ctx)
}

final class KafkaObserver[F[_]: Async](ctx: KafkaContext[F]) {

  def observe(topicName: TopicName): Pipe[F, NJEvent, Nothing] = { (ss: Stream[F, NJEvent]) =>
    ss.map(evt => ProducerRecord(topicName.value, evt.serviceParams.taskParams.taskName, KJson(evt)))
      .chunks
      .through(ctx.topic[String, KJson[NJEvent]](topicName).produce.pipe)
      .drain
  }
}