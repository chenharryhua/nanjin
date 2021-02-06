package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.data.{NonEmptyList, Reader}
import cats.effect._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.interop.reactivestreams._
import fs2.kafka.KafkaByteConsumerRecord
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serde}
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.Materialized

object KafkaChannels {

  final class Fs2Channel[F[_], K, V] private[kafka] (
    val topicName: TopicName,
    codec: KafkaTopicCodec[K, V],
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    updater: Fs2SettingsUpdater[F, K, V]) {
    import fs2.kafka.{
      CommittableConsumerRecord,
      ConsumerSettings,
      Deserializer,
      KafkaConsumer,
      KafkaProducer,
      ProducerRecords,
      ProducerResult,
      ProducerSettings,
      Serializer
    }

    def updateConsumerSettings(
      f: ConsumerSettings[F, Array[Byte], Array[Byte]] => ConsumerSettings[F, Array[Byte], Array[Byte]])
      : Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topicName, codec, kps, kcs, updater.updateConsumer(f))

    def updateProducerSettings(f: ProducerSettings[F, K, V] => ProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topicName, codec, kps, kcs, updater.updateProducer(f))

    def producerSettings(implicit F: Sync[F]): ProducerSettings[F, K, V] =
      updater.producer.run(
        ProducerSettings[F, K, V](Serializer.delegate(codec.keySerializer), Serializer.delegate(codec.valSerializer))
          .withProperties(kps.config))

    def consumerSettings(implicit F: Sync[F]): ConsumerSettings[F, Array[Byte], Array[Byte]] =
      updater.consumer.run(
        ConsumerSettings[F, Array[Byte], Array[Byte]](Deserializer[F, Array[Byte]], Deserializer[F, Array[Byte]])
          .withProperties(kcs.config))

    @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
      new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

    def producerPipe[P](implicit
      F: ConcurrentEffect[F],
      cs: ContextShift[F]): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
      KafkaProducer.pipe[F, K, V, P](producerSettings)

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
    akkaSystem: ActorSystem,
    codec: KafkaTopicCodec[K, V],
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    updater: AkkaSettingsUpdater[K, V]) {
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.{
      CommitterSettings,
      ConsumerMessage,
      ConsumerSettings,
      ProducerMessage,
      ProducerSettings,
      Subscriptions
    }
    import akka.kafka.scaladsl.{Committer, Consumer, Producer}
    import akka.stream.scaladsl.{Flow, Sink, Source}
    import akka.{Done, NotUsed}

    def updateConsumerSettings(
      f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]])
      : AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, updater.updateConsumer(f))

    def updateProducerSettings(f: ProducerSettings[K, V] => ProducerSettings[K, V]): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, updater.updateProducer(f))

    def updateCommitterSettings(f: CommitterSettings => CommitterSettings): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, updater.updateCommitter(f))

    def producerSettings: ProducerSettings[K, V] =
      updater.producer.run(
        ProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer).withProperties(kps.config))

    def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
      updater.consumer.run(
        ConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
          .withProperties(kcs.config))

    def committerSettings: CommitterSettings =
      updater.committer.run(CommitterSettings(akkaSystem))

    @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
      new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

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
    keySerde: Serde[K],
    valueSerde: Serde[V],
    processorName: Option[String],
    timestampExtractor: Option[TimestampExtractor],
    resetPolicy: Option[AutoOffsetReset]) {
    import org.apache.kafka.streams.scala.StreamsBuilder
    import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

    def withProcessorName(pn: String): StreamingChannel[K, V] =
      new StreamingChannel[K, V](topicName, keySerde, valueSerde, Some(pn), timestampExtractor, resetPolicy)

    def withTimestampExtractor(te: TimestampExtractor): StreamingChannel[K, V] =
      new StreamingChannel[K, V](topicName, keySerde, valueSerde, processorName, Some(te), resetPolicy)

    def withResetPololicy(rp: AutoOffsetReset): StreamingChannel[K, V] =
      new StreamingChannel[K, V](topicName, keySerde, valueSerde, processorName, timestampExtractor, Some(rp))

    def consumed: Consumed[K, V] = {
      val base = Consumed.`with`(keySerde, valueSerde)
      val pn   = processorName.fold(base)(pn => base.withName(pn))
      val te   = timestampExtractor.fold(pn)(te => pn.withTimestampExtractor(te))
      resetPolicy.fold(te)(rp => te.withOffsetResetPolicy(rp))
    }

    val kstream: Reader[StreamsBuilder, KStream[K, V]] =
      Reader(builder => builder.stream[K, V](topicName.value)(consumed))

    val ktable: Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName.value)(consumed))

    def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicName.value, mat)(consumed))

    val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicName.value)(consumed))

    def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicName.value, mat)(consumed))
  }
}
