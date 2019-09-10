package com.github.chenharryhua.nanjin.kafka

import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import akka.stream.ActorMaterializer
import cats.Show
import cats.data.{NonEmptyList, Reader}
import cats.effect._
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{KafkaGenericDecoder, _}
import fs2.kafka.{
  CommittableConsumerRecord,
  ConsumerSettings => Fs2ConsumerSettings,
  ProducerSettings => Fs2ProducerSettings
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.kstream.GlobalKTable

object KafkaChannels {

  object Fs2Channel {
    implicit def showFs2Channel[F[_], K, V]: Show[Fs2Channel[F, K, V]] = _.show
  }

  final case class Fs2Channel[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    topicName: String,
    producerSettings: Fs2ProducerSettings[F, K, V],
    consumerSettings: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]],
    keyCodec: KafkaCodec[K],
    valueCodec: KafkaCodec[V]
  ) extends MessagePropertiesFs2 {

    import fs2.Stream
    import fs2.kafka.{consumerStream, KafkaProducer}

    val messageDecoder: KafkaGenericDecoder[CommittableConsumerRecord[F, *, *], K, V] =
      new KafkaGenericDecoder[CommittableConsumerRecord[F, *, *], K, V](keyCodec, valueCodec)

    val messageEncoder: Fs2MessageEncoder[F, K, V] =
      new Fs2MessageEncoder[F, K, V](topicName)

    val recordDecoder: KafkaGenericDecoder[ConsumerRecord, K, V] =
      new KafkaGenericDecoder[ConsumerRecord, K, V](keyCodec, valueCodec)

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

    //  val transactionalProducerStream: Stream[F, TransactionalKafkaProducer[F, K, V]] =
    //    fs2.kafka.transactionalProducerStream[F, K, V](producerSettings)

    val consume: Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      consumerStream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap(_.subscribe(NonEmptyList.of(topicName)))
        .flatMap(_.stream)

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
    keyCodec: KafkaCodec[K],
    valueCodec: KafkaCodec[V],
    materializer: ActorMaterializer)
      extends MessagePropertiesAkka {
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.scaladsl.{Committer, Consumer}
    import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
    import akka.stream.scaladsl.{Flow, Sink, Source}
    import akka.{Done, NotUsed}

    val messageDecoder: KafkaGenericDecoder[CommittableMessage, K, V] =
      new KafkaGenericDecoder[CommittableMessage, K, V](keyCodec, valueCodec)

    val messageEncoder: AkkaMessageEncoder[K, V] =
      new AkkaMessageEncoder[K, V](topicName)

    val recordDecoder: KafkaGenericDecoder[ConsumerRecord, K, V] =
      new KafkaGenericDecoder[ConsumerRecord, K, V](keyCodec, valueCodec)

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

    val plainSink: Sink[ProducerRecord[K, V], F[Done]] =
      akka.kafka.scaladsl.Producer
        .plainSink(producerSettings)
        .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    val commitSink: Sink[ConsumerMessage.Committable, F[Done]] =
      Committer
        .sink(committerSettings)
        .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    def ignoreSink[A]: Sink[A, F[Done]] =
      Sink.ignore.mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    def assign(tps: Map[TopicPartition, Long])
      : Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control] =
      akka.kafka.scaladsl.Consumer
        .plainSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))

    val consume: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
      akka.kafka.scaladsl.Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topicName))

    def record(k: K, v: V): ProducerRecord[K, V] = new ProducerRecord[K, V](topicName, k, v)

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
    topicName: String,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]) {
    import org.apache.kafka.streams.scala.StreamsBuilder
    import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

    val kstream: Reader[StreamsBuilder, KStream[K, V]] =
      Reader(builder => builder.stream[K, V](topicName)(Consumed.`with`(keySerde, valueSerde)))

    val ktable: Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName)(Consumed.`with`(keySerde, valueSerde)))

    val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicName)(Consumed.`with`(keySerde, valueSerde)))

    def ktable(store: KafkaStore.InMemory[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder =>
        builder.table[K, V](topicName, store.materialized)(Consumed.`with`(keySerde, valueSerde)))

    def ktable(store: KafkaStore.Persistent[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder =>
        builder.table[K, V](topicName, store.materialized)(Consumed.`with`(keySerde, valueSerde)))
  }
}
