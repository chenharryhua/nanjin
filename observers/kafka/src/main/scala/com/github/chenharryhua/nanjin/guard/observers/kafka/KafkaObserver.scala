package com.github.chenharryhua.nanjin.guard.observers.kafka

import cats.Endo
import cats.effect.kernel.Async
import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import com.github.chenharryhua.nanjin.kafka.{AvroPair, KafkaContext}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, KJson}
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.generic.JsonCodec

import java.util.UUID

@JsonCodec
final case class EventKey(task: String, service: String)

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] =
    new KafkaObserver[F](ctx, Translator.idTranslator[F])
}

final class KafkaObserver[F[_]](ctx: KafkaContext[F], translator: Translator[F, Event])(implicit F: Async[F])
    extends UpdateTranslator[F, Event, KafkaObserver[F]] {

  def observe(topicName: TopicName): Pipe[F, Event, Event] = {
    def translate(evt: Event): F[Option[ProducerRecord[KJson[EventKey], KJson[Event]]]] =
      translator
        .translate(evt)
        .map(
          _.map(evt =>
            ProducerRecord(
              topicName.value,
              KJson(EventKey(evt.serviceParams.taskName.value, evt.serviceParams.serviceName.value)),
              KJson(evt))))

    val pair = AvroPair(AvroFor[KJson[EventKey]], AvroFor[KJson[Event]])

    (ss: Stream[F, Event]) =>
      for {
        client <- Stream.resource(ctx.sharedProduce(pair).clientR)
        ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- ss
          .evalTap(ofm.monitoring)
          .evalTap(translate(_).flatMap(_.traverse(client.produceOne(_).flatten)))
          .onFinalize(ofm.terminated.flatMap(client.produce(_).flatten).void)
      } yield event
  }

  def observe(topicName: TopicNameL): Pipe[F, Event, Event] =
    observe(TopicName(topicName))

  override def updateTranslator(f: Endo[Translator[F, Event]]): KafkaObserver[F] =
    new KafkaObserver(ctx, f(translator))
}
