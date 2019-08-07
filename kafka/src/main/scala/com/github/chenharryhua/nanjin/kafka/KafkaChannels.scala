package com.github.chenharryhua.nanjin.kafka

import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import akka.stream.ActorMaterializer
import cats.Show
import cats.data.Reader
import cats.effect._
import cats.implicits._
import fs2.kafka.{ConsumerSettings => Fs2ConsumerSettings, ProducerSettings => Fs2ProducerSettings}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.kstream.GlobalKTable

import scala.util.{Success, Try}

object Fs2Channel {
  implicit def showFs2Channel[F[_], K, V]: Show[Fs2Channel[F, K, V]] = _.show
}
final case class Fs2Channel[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
  topicName: String,
  producerSettings: Fs2ProducerSettings[F, K, V],
  consumerSettings: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]],
  keyIso: Iso[Array[Byte], K],
  valueIso: Iso[Array[Byte], V]
) extends Fs2MessageBitraverse {
  import fs2.Stream
  import fs2.kafka._

  def updateProducerSettings(
    f: Fs2ProducerSettings[F, K, V] => Fs2ProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
    copy(producerSettings = f(producerSettings))

  def updateConsumerSettings(
    f: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] => Fs2ConsumerSettings[
      F,
      Array[Byte],
      Array[Byte]]): Fs2Channel[F, K, V] =
    copy(consumerSettings = f(consumerSettings))

  val producerStream: Stream[F, KafkaProducer[F, K, V]] =
    fs2.kafka.producerStream[F, K, V](producerSettings)

  val transactionalProducerStream: Stream[F, TransactionalKafkaProducer[F, K, V]] =
    fs2.kafka.transactionalProducerStream[F, K, V](producerSettings)

  val consume: Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    consumerStream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.subscribeTo(topicName))
      .flatMap(_.stream)

  val consumeNativeMessages: Stream[F, CommittableConsumerRecord[F, Try[K], Try[V]]] =
    consume.map(_.bimap(k => Try(keyIso.get(k)), v => Try(valueIso.get(v))))

  val consumeMessages: Stream[F, Try[CommittableConsumerRecord[F, K, V]]] =
    consume.map(_.bitraverse(k => Try(keyIso.get(k)), v => Try(valueIso.get(v))))

  val consumeValidMessages: Stream[F, CommittableConsumerRecord[F, K, V]] =
    consumeMessages.map(_.map(_.bitraverse(Option(_), Option(_)))).collect {
      case Success(Some(x)) => x
    }

  val consumeValues: Stream[F, Try[CommittableConsumerRecord[F, Array[Byte], V]]] =
    consume.map(_.bitraverse(k => Success(k), v => Try(valueIso.get(v))))

  val consumeValidValues: Stream[F, CommittableConsumerRecord[F, Array[Byte], V]] =
    consumeValues.map(_.map(_.bitraverse(Some(_), Option(_)))).collect {
      case Success(Some(x)) => x
    }

  val consumeKeys: Stream[F, Try[CommittableConsumerRecord[F, K, Array[Byte]]]] =
    consume.map(_.bitraverse(k => Try(keyIso.get(k)), v => Success(v)))

  val consumeValidKeys: Stream[F, CommittableConsumerRecord[F, K, Array[Byte]]] =
    consumeKeys.map(_.map(_.bitraverse(Option(_), Some(_)))).collect {
      case Success(Some(x)) => x
    }

  val show: String =
    s"""
       |fs2 consumer runtime settings:
       |${consumerSettings.show}
       |${consumerSettings.properties.show}
       |
       |fs2 producer runtime settings:
       |${producerSettings.show}
       |${producerSettings.properties.show}""".stripMargin
}

object AkkaChannel {
  implicit def showAkkaChannel[F[_], K, V]: Show[AkkaChannel[F, K, V]] = _.show
}
final case class AkkaChannel[F[_]: ContextShift: Async, K, V](
  topicName: String,
  producerSettings: AkkaProducerSettings[K, V],
  consumerSettings: AkkaConsumerSettings[Array[Byte], Array[Byte]],
  committerSettings: AkkaCommitterSettings,
  keyIso: Iso[Array[Byte], K],
  valueIso: Iso[Array[Byte], V],
  materializer: ActorMaterializer)
    extends AkkaMessageBitraverse {
  import akka.kafka.ConsumerMessage.CommittableMessage
  import akka.kafka.ProducerMessage.Envelope
  import akka.kafka.scaladsl.{Committer, Consumer}
  import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
  import akka.stream.scaladsl.{Flow, Sink, Source}
  import akka.{Done, NotUsed}

  def updateProducerSettings(
    f: AkkaProducerSettings[K, V] => AkkaProducerSettings[K, V]): AkkaChannel[F, K, V] =
    copy(producerSettings = f(producerSettings))

  def updateConsumerSettings(
    f: AkkaConsumerSettings[Array[Byte], Array[Byte]] => AkkaConsumerSettings[
      Array[Byte],
      Array[Byte]]): AkkaChannel[F, K, V] =
    copy(consumerSettings = f(consumerSettings))

  def updateCommitterSettings(
    f: AkkaCommitterSettings => AkkaCommitterSettings): AkkaChannel[F, K, V] =
    copy(committerSettings = f(committerSettings))

  val committableSink: Sink[Envelope[K, V, ConsumerMessage.Committable], F[Done]] =
    akka.kafka.scaladsl.Producer
      .committableSink(producerSettings)
      .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

  def flexiFlow[P]: Flow[Envelope[K, V, P], ProducerMessage.Results[K, V, P], NotUsed] =
    akka.kafka.scaladsl.Producer.flexiFlow[K, V, P](producerSettings)

  def plainSink: Sink[ProducerRecord[K, V], F[Done]] =
    akka.kafka.scaladsl.Producer
      .plainSink(producerSettings)
      .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

  val commitSink: Sink[ConsumerMessage.Committable, F[Done]] =
    Committer.sink(committerSettings).mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

  def ignoreSink[A]: Sink[A, F[Done]] =
    Sink.ignore.mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

  def assign(tps: Map[TopicPartition, Long])
    : Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control] =
    akka.kafka.scaladsl.Consumer
      .plainSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))

  val consume: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
    akka.kafka.scaladsl.Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topicName))

  val consumeNativeMessages: Source[CommittableMessage[Try[K], Try[V]], Consumer.Control] =
    consume.map(_.bimap(k => Try(keyIso.get(k)), v => Try(valueIso.get(v))))

  val consumeMessages: Source[Try[CommittableMessage[K, V]], Consumer.Control] =
    consume.map(_.bitraverse(k => Try(keyIso.get(k)), v => Try(valueIso.get(v))))

  val consumeValidMessages: Source[CommittableMessage[K, V], Consumer.Control] =
    consumeMessages.map(_.map(_.bitraverse(Option(_), Option(_)))).collect {
      case Success(Some(x)) => x
    }

  val consumeValues: Source[Try[CommittableMessage[Array[Byte], V]], Consumer.Control] =
    consume.map(_.bitraverse(k => Success(k), v => Try(valueIso.get(v))))

  val consumeValidValues: Source[CommittableMessage[Array[Byte], V], Consumer.Control] =
    consumeValues.map(_.map(_.bitraverse(Some(_), Option(_)))).collect {
      case Success(Some(x)) => x
    }

  val consumeKeys: Source[Try[CommittableMessage[K, Array[Byte]]], Consumer.Control] =
    consume.map(_.bitraverse(k => Try(keyIso.get(k)), v => Success(v)))

  val consumeValidKeys: Source[CommittableMessage[K, Array[Byte]], Consumer.Control] =
    consumeKeys.map(_.map(_.bitraverse(Option(_), Some(_)))).collect { case Success(Some(x)) => x }

  val show: String =
    s"""
       |akka consumer runtime settings:
       |${consumerSettings.toString()}
       |
       |akka producer runtime settings:
       |${producerSettings.toString()}
     """.stripMargin
}

final case class StreamingChannel[K, V](
  topicDef: TopicDef[K, V],
  keySerde: KeySerde[K],
  valueSerde: ValueSerde[V]) {
  import org.apache.kafka.streams.scala.StreamsBuilder
  import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

  val kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(
      builder => builder.stream[K, V](topicDef.topicName)(Consumed.`with`(keySerde, valueSerde)))

  val ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(
      builder => builder.table[K, V](topicDef.topicName)(Consumed.`with`(keySerde, valueSerde)))

  val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder =>
      builder.globalTable[K, V](topicDef.topicName)(Consumed.`with`(keySerde, valueSerde)))

  def ktable(store: KafkaStore.InMemory[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(
      builder =>
        builder.table[K, V](topicDef.topicName, store.materialized)(
          Consumed.`with`(keySerde, valueSerde)))

  def ktable(store: KafkaStore.Persistent[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(
      builder =>
        builder.table[K, V](topicDef.topicName, store.materialized)(
          Consumed.`with`(keySerde, valueSerde)))
}
