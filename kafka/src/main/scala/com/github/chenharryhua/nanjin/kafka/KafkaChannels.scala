package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.{Done, NotUsed}
import cats.data.{NonEmptyList, Reader}
import cats.effect.kernel.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.interop.reactivestreams.*
import fs2.kafka.KafkaByteConsumerRecord
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serde}
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.Materialized

object KafkaChannels {

  /** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
    */
  final class Fs2Channel[F[_], K, V] private[kafka] (
    val topicName: TopicName,
    codec: KafkaTopicCodec[K, V],
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    csUpdater: fs2Updater.Consumer[F],
    psUpdater: fs2Updater.Producer[F, K, V]) {
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

    // settings

    def updateConsumer(
      f: ConsumerSettings[F, Array[Byte], Array[Byte]] => ConsumerSettings[F, Array[Byte], Array[Byte]])
      : Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topicName, codec, kps, kcs, csUpdater.updateConfig(f), psUpdater)

    def updateProducer(f: ProducerSettings[F, K, V] => ProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topicName, codec, kps, kcs, csUpdater, psUpdater.updateConfig(f))

    def producerSettings(implicit F: Sync[F]): ProducerSettings[F, K, V] =
      psUpdater.settings.run(
        ProducerSettings[F, K, V](Serializer.delegate(codec.keySerializer), Serializer.delegate(codec.valSerializer))
          .withProperties(kps.config))

    def consumerSettings(implicit F: Sync[F]): ConsumerSettings[F, Array[Byte], Array[Byte]] =
      csUpdater.settings.run(
        ConsumerSettings[F, Array[Byte], Array[Byte]](Deserializer[F, Array[Byte]], Deserializer[F, Array[Byte]])
          .withProperties(kcs.config))

    @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
      new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

    // pipe
    def producerPipe[P](implicit F: Async[F]): Pipe[F, ProducerRecords[P, K, V], ProducerResult[P, K, V]] =
      KafkaProducer.pipe[F, K, V, P](producerSettings)

    // sources
    def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      KafkaConsumer
        .stream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
        .flatMap(_.stream)

    def assign(tps: KafkaTopicPartition[KafkaOffset])(implicit
      F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
      if (tps.isEmpty)
        Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]
      else
        KafkaConsumer
          .stream[F, Array[Byte], Array[Byte]](consumerSettings)
          .evalTap { c =>
            c.assign(topicName.value) *> tps.value.toList.traverse { case (tp, offset) =>
              c.seek(tp, offset.offset.value)
            }
          }
          .flatMap(_.stream)

  }

  /** [[https://doc.akka.io/docs/alpakka-kafka/current/home.html]]
    */
  final class AkkaChannel[F[_], K, V] private[kafka] (
    val topicName: TopicName,
    akkaSystem: ActorSystem,
    codec: KafkaTopicCodec[K, V],
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    csUpdater: akkaUpdater.Consumer,
    psUpdater: akkaUpdater.Producer[K, V],
    ctUpdater: akkaUpdater.Committer) {
    import akka.kafka.*
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.scaladsl.{Committer, Consumer, Producer, Transactional}
    import akka.stream.scaladsl.{Flow, Sink, Source}

    // settings
    def updateConsumer(f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]])
      : AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, csUpdater.updateConfig(f), psUpdater, ctUpdater)

    def updateProducer(f: ProducerSettings[K, V] => ProducerSettings[K, V]): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, csUpdater, psUpdater.updateConfig(f), ctUpdater)

    def updateCommitter(f: CommitterSettings => CommitterSettings): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topicName, akkaSystem, codec, kps, kcs, csUpdater, psUpdater, ctUpdater.updateConfig(f))

    def producerSettings: ProducerSettings[K, V] =
      psUpdater.settings.run(
        ProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer).withProperties(kps.config))

    def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
      csUpdater.settings.run(
        ConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
          .withProperties(kcs.config))

    def committerSettings: CommitterSettings =
      ctUpdater.settings.run(CommitterSettings(akkaSystem))

    @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
      new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

    // sinks
    def flexiFlow[P]: Flow[Envelope[K, V, P], ProducerMessage.Results[K, V, P], NotUsed] =
      Producer.flexiFlow[K, V, P](producerSettings)

    def committableSink(implicit F: Async[F]): Sink[Envelope[K, V, ConsumerMessage.Committable], F[Done]] =
      Producer.committableSink(producerSettings, committerSettings).mapMaterializedValue(f => F.fromFuture(F.pure(f)))

    def plainSink(implicit F: Async[F]): Sink[ProducerRecord[K, V], F[Done]] =
      Producer.plainSink(producerSettings).mapMaterializedValue(f => F.fromFuture(F.pure(f)))

    def commitSink(implicit F: Async[F]): Sink[ConsumerMessage.Committable, F[Done]] =
      Committer.sink(committerSettings).mapMaterializedValue(f => F.fromFuture(F.pure(f)))

    // sources

    def assign(tps: KafkaTopicPartition[KafkaOffset]): Source[KafkaByteConsumerRecord, Consumer.Control] =
      if (tps.isEmpty)
        Source.empty.mapMaterializedValue(_ => Consumer.NoopControl)
      else
        Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(tps.value.mapValues(_.offset.value)))

    val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName.value))

    def stream(implicit F: Async[F]): Stream[F, CommittableMessage[Array[Byte], Array[Byte]]] =
      Stream.suspend(source.runWith(Sink.asPublisher(fanout = false))(Materializer(akkaSystem)).toStream[F])

    val transactionalSource: Source[ConsumerMessage.TransactionalMessage[K, V], Consumer.Control] =
      Transactional.source(consumerSettings, Subscriptions.topics(topicName.value)).map(decoder(_).decode)

  }

  final class StreamingChannel[K, V] private[kafka] (topicDef: TopicDef[K, V]) {
    import org.apache.kafka.streams.scala.StreamsBuilder
    import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

    val kstream: Reader[StreamsBuilder, KStream[K, V]] =
      Reader(builder => builder.stream[K, V](topicDef.topicName.value)(topicDef.consumed))

    val ktable: Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicDef.topicName.value)(topicDef.consumed))

    def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
      Reader(builder => builder.table[K, V](topicDef.topicName.value, mat)(topicDef.consumed))

    val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicDef.topicName.value)(topicDef.consumed))

    def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
      Reader(builder => builder.globalTable[K, V](topicDef.topicName.value, mat)(topicDef.consumed))
  }
}
