package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings
}
import akka.stream.Materializer
import cats.data.{NonEmptyList, Reader}
import cats.effect._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import fs2.interop.reactivestreams._
import fs2.kafka.{
  KafkaByteConsumerRecord,
  KafkaConsumer,
  KafkaProducer,
  ProducerRecords,
  ProducerResult,
  ConsumerSettings => Fs2ConsumerSettings,
  ProducerSettings => Fs2ProducerSettings
}
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.Materialized

object KafkaChannels {

  final class Fs2Channel[F[_], K, V] private[kafka] (
    val topicName: TopicName,
    val producerSettings: Fs2ProducerSettings[F, K, V],
    val consumerSettings: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]]) {

    import fs2.kafka.CommittableConsumerRecord

    def producerPipe[P](implicit
      cs: ContextShift[F],
      F: ConcurrentEffect[F]): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
      KafkaProducer.pipe(producerSettings)

    def stream(implicit
      cs: ContextShift[F],
      timer: Timer[F],
      F: ConcurrentEffect[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      KafkaConsumer
        .stream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
        .flatMap(_.stream)

    def assign(tps: Map[TopicPartition, Long])(implicit
      cs: ContextShift[F],
      timer: Timer[F],
      F: ConcurrentEffect[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      if (tps.isEmpty)
        Stream.empty
      else
        KafkaConsumer
          .stream[F, Array[Byte], Array[Byte]](consumerSettings)
          .evalTap { c =>
            c.assign(topicName.value) *> tps.toList.traverse { case (tp, offset) =>
              c.seek(tp, offset)
            }
          }
          .flatMap(_.stream)
  }

  final class AkkaChannel[F[_], K, V] private[kafka] (
    val topicName: TopicName,
    val producerSettings: AkkaProducerSettings[K, V],
    val consumerSettings: AkkaConsumerSettings[Array[Byte], Array[Byte]],
    val committerSettings: AkkaCommitterSettings,
    val akkaSystem: ActorSystem) {
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.scaladsl.{Committer, Consumer, Producer}
    import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
    import akka.stream.scaladsl.{Flow, Sink, Source}
    import akka.{Done, NotUsed}

    def flexiFlow[P]: Flow[Envelope[K, V, P], ProducerMessage.Results[K, V, P], NotUsed] =
      Producer.flexiFlow[K, V, P](producerSettings)

    def committableSink(implicit
      cs: ContextShift[F],
      F: Async[F]): Sink[Envelope[K, V, ConsumerMessage.Committable], F[Done]] =
      Producer
        .committableSink(producerSettings, committerSettings)
        .mapMaterializedValue(f => Async.fromFuture(F.pure(f)))

    def plainSink(implicit cs: ContextShift[F], F: Async[F]): Sink[ProducerRecord[K, V], F[Done]] =
      Producer.plainSink(producerSettings).mapMaterializedValue(f => Async.fromFuture(F.pure(f)))

    def commitSink(implicit cs: ContextShift[F], F: Async[F]): Sink[ConsumerMessage.Committable, F[Done]] =
      Committer.sink(committerSettings).mapMaterializedValue(f => Async.fromFuture(F.pure(f)))

    def assign(tps: Map[TopicPartition, Long]): Source[KafkaByteConsumerRecord, Consumer.Control] =
      Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))

    val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName.value))

    def stream(implicit F: ConcurrentEffect[F]): Stream[F, CommittableMessage[Array[Byte], Array[Byte]]] =
      Stream.suspend(source.runWith(Sink.asPublisher(fanout = false))(Materializer(akkaSystem)).toStream[F])

  }

  final class StreamingChannel[K, V] private[kafka] (
    val topicName: TopicName,
    val keySerde: Serde[K],
    val valueSerde: Serde[V]) {
    import org.apache.kafka.streams.scala.StreamsBuilder
    import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

    val kstream: Reader[StreamsBuilder, KStream[K, V]] =
      Reader(builder => builder.stream[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    val ktable: Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName.value, mat)(Consumed.`with`(keySerde, valueSerde)))

    val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicName.value, mat)(Consumed.`with`(keySerde, valueSerde)))
  }
}
