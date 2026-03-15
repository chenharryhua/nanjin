package com.github.chenharryhua.nanjin.guard.observers.kafka

import cats.Endo
import cats.effect.kernel.Async
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicName}
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{Translator, UpdateTranslator}
import com.github.chenharryhua.nanjin.kafka.AvroForPair
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroFor
import fs2.kafka.ProducerRecord
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.Codec
import io.circe.Json
import io.circe.syntax.EncoderOps

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

    val pair = AvroForPair(AvroFor[Json], AvroFor[Json])

    (ss: Stream[F, Event]) =>
      for {
        client <- Stream.resource(ctx.sharedProduce(pair).clientR)
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
