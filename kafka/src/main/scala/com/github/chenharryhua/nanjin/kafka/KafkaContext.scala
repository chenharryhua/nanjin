package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data.Reader
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.{Eval, Show}
import com.github.chenharryhua.nanjin.kafka.codec.KafkaSerde
import com.github.chenharryhua.nanjin.kafka.codec.{KafkaSerde, SerdeOf}
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import fs2.Stream
import fs2.kafka._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

sealed class KafkaContext[F[_]](val settings: KafkaSettings)(
  implicit
  val timer: Timer[F],
  val contextShift: ContextShift[F],
  val concurrentEffect: ConcurrentEffect[F]) {
  final def asKey[K: SerdeOf]: KafkaSerde.Key[K] =
    SerdeOf[K].asKey(settings.schemaRegistrySettings.config)

  final def asValue[V: SerdeOf]: KafkaSerde.Value[V] =
    SerdeOf[V].asValue(settings.schemaRegistrySettings.config)

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[K, V] =
    new KafkaTopic[K, V](KafkaTopicDescription(topicDef, settings))

  final def topic[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf](
    topicName: String): KafkaTopic[K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  final def kafkaStreams(topology: Reader[StreamsBuilder, Unit]): Stream[F, KafkaStreams] =
    new KafkaStreamRunner[F](settings.streamSettings).stream(topology)
}

final class IoKafkaContext(settings: KafkaSettings)(implicit cs: ContextShift[IO], timer: Timer[IO])
    extends KafkaContext[IO](settings) {}

final class ZioKafkaContext(settings: KafkaSettings)(
  implicit cs: ContextShift[zio.Task],
  timer: Timer[zio.Task],
  ce: ConcurrentEffect[zio.Task]
) extends KafkaContext[zio.Task](settings) {}

final class MonixKafkaContext(settings: KafkaSettings)(
  implicit cs: ContextShift[monix.eval.Task],
  timer: Timer[monix.eval.Task],
  ce: ConcurrentEffect[monix.eval.Task])
    extends KafkaContext[monix.eval.Task](settings) {}
