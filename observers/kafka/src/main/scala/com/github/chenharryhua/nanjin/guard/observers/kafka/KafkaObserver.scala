package com.github.chenharryhua.nanjin.guard.observers.kafka

import cats.Endo
import cats.effect.kernel.Async
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef, TopicName}
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer

final case class EventKey(task: String, service: String) derives Codec.AsObject

object KafkaObserver {
  def apply[F[_]: Async](ctx: KafkaContext[F]): KafkaObserver[F] =
    new KafkaObserver[F](ctx, Translator.idTranslator[F])
}

final class KafkaObserver[F[_]](ctx: KafkaContext[F], translator: Translator[F, Event])(using F: Async[F])
    extends UpdateTranslator[F, Event, KafkaObserver[F]] {

  private val name: String = "Kafka Observer"

  def observe(topicName: TopicName): Pipe[F, Event, Event] = {
    def translate(evt: Event): F[Option[ProducerRecord[Json, Json]]] =
      translator
        .translate(evt)
        .map(
          _.map(evt =>
            ProducerRecord(
              topicName.value,
              EventKey(evt.serviceParams.taskName.value, evt.serviceParams.serviceName.value).asJson,
              evt.asJson)))
    val topic = TopicDef(topicName, Primitive[ByteBuffer].become[Json], Primitive[ByteBuffer].become[Json])
    (ss: Stream[F, Event]) =>
      for {
        client <- ctx.produce(topic).clientS
        log <- Stream.eval(Slf4jLogger.create[F])
        _ <- Stream.eval(log.info(s"initialize $name"))
        ofm <- Stream.eval(
          F.ref[Map[ServiceId, ServiceStart]](Map.empty).map(new FinalizeMonitor(translate, _)))
        event <- ss
          .evalTap(ofm.monitoring)
          .evalTap {
            translate(_)
              .flatMap(_.traverse(client.produceOne(_).flatten))
              .void
              .recoverWith(ex => log.error(ex)(name))
          }
          .onFinalize {
            ofm.terminated.flatMap(client.produce(_).flatten) *>
              log.info(s"$name was closed")
          }
      } yield event
  }

  override def updateTranslator(f: Endo[Translator[F, Event]]): KafkaObserver[F] =
    new KafkaObserver(ctx, f(translator))
}
