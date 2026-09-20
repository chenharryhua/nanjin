package com.github.chenharryhua.nanjin.guard.observers.kafka

import cats.effect.kernel.Async
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import cats.{Endo, Parallel}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef}
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger

final private case class EventKey(task: String, service: String) derives Codec.AsObject

sealed trait KafkaObserver[F[_]] extends UpdateTranslator[F, Event, KafkaObserver[F]] {

  /** Transform the event translator, e.g. to filter or reshape events before producing them. */
  override def withTranslator(f: Endo[Translator[F, Event]]): KafkaObserver[F]

  /** Build a pipe that produces each event to `topicName` as a JSON key/value record. Events pass through
    * unchanged; produce failures are logged rather than raised.
    *
    * @param topicName
    *   the Kafka topic to produce to.
    */
  def observe(topicName: String): Pipe[F, Event, Event]
}

object KafkaObserver {
  def apply[F[_]: {Async, Parallel}](ctx: KafkaContext[F]): KafkaObserver[F] =
    new KafkaObserverImpl[F](ctx, Translator.idTranslator[F])
}

final private class KafkaObserverImpl[F[_]: Parallel](ctx: KafkaContext[F], translator: Translator[F, Event])(
  using F: Async[F])
    extends KafkaObserver[F] {

  private val NAME: String = "Kafka Observer"

  override def observe(topicName: String): Pipe[F, Event, Event] = {
    def translate(evt: Event): F[Option[ProducerRecord[Json, Json]]] =
      translator
        .translate(evt)
        .map(
          _.map(evt =>
            ProducerRecord(
              topicName,
              EventKey(evt.serviceIdentity.task.value, evt.serviceIdentity.service.value).asJson,
              evt.asJson)))
    val topic = TopicDef(topicName, Structured[Json], Structured[Json])
    (ss: Stream[F, Event]) =>
      for {
        client <- ctx.produce(topic).clientS
        log <- Stream.eval(Slf4jLogger.create[F])
        _ <- Stream.eval(log.info(s"initialize $NAME"))
        ofm <- Stream.eval(FinalizeMonitor[F])
        event <- ss
          .evalTap(ofm.monitoring)
          .evalTap {
            translate(_)
              .flatMap(_.traverse(client.produceOne(_).flatten))
              .void
              .recoverWith(ex => log.error(ex)(NAME))
          }
          .onFinalize {
            ofm.terminated.flatMap(_.traverseFilter(translate).flatMap(client.produce(_).flatten)) *>
              log.info(s"$NAME was closed")
          }
      } yield event
  }

  override def withTranslator(f: Endo[Translator[F, Event]]): KafkaObserver[F] =
    new KafkaObserverImpl(ctx, f(translator))
}
