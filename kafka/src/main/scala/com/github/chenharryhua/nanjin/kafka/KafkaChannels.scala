package com.github.chenharryhua.nanjin.kafka

import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import akka.stream.Materializer
import cats.data.{NonEmptyList, Reader}
import cats.effect._
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.codec.NJSerde
import com.github.chenharryhua.nanjin.kafka.common.{KafkaOffsetRange, TopicName}
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import fs2.interop.reactivestreams._
import fs2.kafka.{ConsumerSettings => Fs2ConsumerSettings, ProducerSettings => Fs2ProducerSettings}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.kstream.GlobalKTable

object KafkaChannels {

  final class Fs2Channel[F[_]: ContextShift: Timer: ConcurrentEffect, K, V] private[kafka] (
    topicName: TopicName,
    producerSettings: Fs2ProducerSettings[F, K, V],
    consumerSettings: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]]) {

    import fs2.kafka.{consumerStream, CommittableConsumerRecord, KafkaProducer}

    def withProducerSettings(
      f: Fs2ProducerSettings[F, K, V] => Fs2ProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
      new Fs2Channel(topicName, f(producerSettings), consumerSettings)

    def withConsumerSettings(
      f: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] => Fs2ConsumerSettings[
        F,
        Array[Byte],
        Array[Byte]]): Fs2Channel[F, K, V] =
      new Fs2Channel(topicName, producerSettings, f(consumerSettings))

    val producerStream: Stream[F, KafkaProducer[F, K, V]] =
      fs2.kafka.producerStream[F, K, V](producerSettings)

    val consume: Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      consumerStream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
        .flatMap(_.stream)

    def assign(tps: Map[TopicPartition, Long])
      : Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      Keyboard.signal.flatMap { signal =>
        consumerStream[F, Array[Byte], Array[Byte]](consumerSettings).evalTap { c =>
          c.assign(topicName.value) *> tps.toList.traverse {
            case (tp, offset) => c.seek(tp, offset)
          }
        }.flatMap(_.stream)
          .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
          .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
      }
  }

  final class AkkaChannel[F[_]: ContextShift, K, V] private[kafka] (
    kit: KafkaTopicKit[K, V],
    producerSettings: AkkaProducerSettings[K, V],
    consumerSettings: AkkaConsumerSettings[Array[Byte], Array[Byte]],
    committerSettings: AkkaCommitterSettings)(implicit F: ConcurrentEffect[F]) {
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.scaladsl.{Committer, Consumer, Producer}
    import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
    import akka.stream.scaladsl.{Flow, Sink, Source}
    import akka.{Done, NotUsed}

    val topicName: TopicName = kit.topicName

    def withProducerSettings(
      f: AkkaProducerSettings[K, V] => AkkaProducerSettings[K, V]): AkkaChannel[F, K, V] =
      new AkkaChannel(kit, f(producerSettings), consumerSettings, committerSettings)

    def withConsumerSettings(
      f: AkkaConsumerSettings[Array[Byte], Array[Byte]] => AkkaConsumerSettings[
        Array[Byte],
        Array[Byte]]): AkkaChannel[F, K, V] =
      new AkkaChannel(kit, producerSettings, f(consumerSettings), committerSettings)

    def withCommitterSettings(
      f: AkkaCommitterSettings => AkkaCommitterSettings): AkkaChannel[F, K, V] =
      new AkkaChannel(kit, producerSettings, consumerSettings, f(committerSettings))

    def flexiFlow[P]: Flow[Envelope[K, V, P], ProducerMessage.Results[K, V, P], NotUsed] =
      Producer.flexiFlow[K, V, P](producerSettings)

    val committableSink: Sink[Envelope[K, V, ConsumerMessage.Committable], F[Done]] =
      Producer
        .committableSink(producerSettings, committerSettings)
        .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    val plainSink: Sink[ProducerRecord[K, V], F[Done]] =
      Producer
        .plainSink(producerSettings)
        .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    val commitSink: Sink[ConsumerMessage.Committable, F[Done]] =
      Committer
        .sink(committerSettings)
        .mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

    def assign(tps: Map[TopicPartition, Long])
      : Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control] =
      Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))

    val consume: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName.value))

    def fs2Consume(
      implicit mat: Materializer): Stream[F, CommittableMessage[Array[Byte], Array[Byte]]] =
      consume.runWith(Sink.asPublisher(fanout = false)).toStream[F]

    def timeRanged(dateTimeRange: NJDateTimeRange)(
      implicit mat: Materializer): Stream[F, ConsumerRecord[Array[Byte], Array[Byte]]] = {
      val exec = for {
        offsetRange <- KafkaConsumerApi[F, K, V](kit)
          .use(_.offsetRangeFor(dateTimeRange).map(_.flatten[KafkaOffsetRange]))
      } yield {
        val totalSize   = offsetRange.mapValues(_.distance).value.values.sum
        val endPosition = offsetRange.mapValues(_.until.value)
        assign(offsetRange.value.mapValues(_.from.value))
          .groupBy(8, _.partition)
          .takeWhile(m => endPosition.get(m.topic, m.partition).exists(m.offset < _))
          .mergeSubstreams
          .take(totalSize)
          .runWith(Sink.asPublisher(fanout = false))
          .toStream[F]
      }
      Stream.force(exec)
    }
  }

  final class StreamingChannel[K, V] private[kafka] (
    topicName: TopicName,
    keySerde: NJSerde[K],
    valueSerde: NJSerde[V]) {
    import org.apache.kafka.streams.scala.StreamsBuilder
    import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

    val kstream: Reader[StreamsBuilder, KStream[K, V]] =
      Reader(builder =>
        builder.stream[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    val ktable: Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder =>
        builder.globalTable[K, V](topicName.value)(Consumed.`with`(keySerde, valueSerde)))

    def ktable(store: KafkaStore.InMemory[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder =>
        builder.table[K, V](topicName.value, store.materialized)(
          Consumed.`with`(keySerde, valueSerde)))

    def ktable(store: KafkaStore.Persistent[K, V]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder =>
        builder.table[K, V](topicName.value, store.materialized)(
          Consumed.`with`(keySerde, valueSerde)))
  }
}
