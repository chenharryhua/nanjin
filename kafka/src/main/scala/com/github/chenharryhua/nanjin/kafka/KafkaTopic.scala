package com.github.chenharryhua.nanjin.kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.{
  ProducerMessage,
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import cats.Traverse
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import fs2.kafka.{
  producerResource,
  ConsumerSettings => Fs2ConsumerSettings,
  Deserializer     => Fs2Deserializer,
  KafkaProducer    => Fs2KafkaProducer,
  ProducerRecord   => Fs2ProducerRecord,
  ProducerRecords  => Fs2ProducerRecords,
  ProducerResult   => Fs2ProducerResult,
  ProducerSettings => Fs2ProducerSettings,
  Serializer       => Fs2Serializer
}
import io.circe.Json
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, Serializer}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

import scala.collection.immutable
import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val settings: KafkaSettings)
    extends TopicNameExtractor[K, V] with Serializable {
  import topicDef.{serdeOfKey, serdeOfVal}

  val topicName: TopicName = topicDef.topicName

  def withGroupId(gid: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, settings.withGroupId(gid))

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  //need to reconstruct codec when working in spark
  @transient lazy val codec: TopicCodec[K, V] = new TopicCodec(
    serdeOfKey.asKey(settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfVal.asValue(settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def fs2ProducerSettings(implicit ev: Sync[F]): Fs2ProducerSettings[F, K, V] =
    Fs2ProducerSettings[F, K, V](
      Fs2Serializer.delegate(codec.keySerializer),
      Fs2Serializer.delegate(codec.valSerializer)).withProperties(settings.producerSettings.config)

  def fs2ConsumerSettings(implicit ev: Sync[F]): Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
    Fs2ConsumerSettings[F, Array[Byte], Array[Byte]](
      Fs2Deserializer[F, Array[Byte]],
      Fs2Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.config)

  def akkaProducerSettings(akkaSystem: ActorSystem): AkkaProducerSettings[K, V] =
    AkkaProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer)
      .withProperties(settings.producerSettings.config)

  def akkaConsumerSettings(
    akkaSystem: ActorSystem): AkkaConsumerSettings[Array[Byte], Array[Byte]] = {
    val byteArrayDeserializer = new ByteArrayDeserializer
    AkkaConsumerSettings[Array[Byte], Array[Byte]](
      akkaSystem,
      byteArrayDeserializer,
      byteArrayDeserializer).withProperties(settings.consumerSettings.config)
  }

  def akkaCommitterSettings(akkaSystem: ActorSystem): AkkaCommitterSettings =
    AkkaCommitterSettings(akkaSystem)

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def toJackson[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Json =
    topicDef.toJackson(decoder(cr).record)

  def fromJackson(jsonString: String): Try[NJConsumerRecord[K, V]] =
    topicDef.fromJackson(jsonString)

  def fs2PR(k: K, v: V): Fs2ProducerRecord[K, V] = Fs2ProducerRecord(topicName.value, k, v)

  def akkaProducerRecords(key: K, value: V): ProducerMessage.Envelope[K, V, NotUsed] =
    ProducerMessage.single[K, V](new ProducerRecord[K, V](topicDef.topicName.value, key, value))

  def akkaProducerRecords[P](key: K, value: V, p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.single[K, V, P](
      new ProducerRecord[K, V](topicDef.topicName.value, key, value),
      p)

  def akkaProducerRecord(seq: immutable.Seq[(K, V)]): ProducerMessage.Envelope[K, V, NotUsed] =
    ProducerMessage.multi(seq.map {
      case (k, v) => new ProducerRecord(topicDef.topicName.value, k, v)
    })

  def akkaProducerRecord[P](seq: immutable.Seq[(K, V)], p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.multi(seq.map {
      case (k, v) => new ProducerRecord(topicDef.topicName.value, k, v)
    }, p)

  override def toString: String = {
    import cats.derived.auto.show._
    s"""
    |topic: $topicName
    |consumer-group-id: ${settings.groupId}
    |stream-app-id:     ${settings.appId}
    |settings:
    |${settings.consumerSettings.show}
    |${settings.producerSettings.show}
    |${settings.schemaRegistrySettings.show}
    |${settings.adminSettings.show}
    |${settings.streamSettings.show}
    |
    |${codec.keySerde.tag}:
    |${codec.keySerde.configProps}
    |${codec.keySchema.toString(true)}
    |
    |${codec.valSerde.tag}:
    |${codec.valSerde.configProps}
    |${codec.valSchema.toString(true)}
   """
  }

  //channels
  def fs2Channel(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDef.topicName,
      fs2ProducerSettings,
      fs2ConsumerSettings)

  def akkaChannel(
    implicit
    akkaSystem: ActorSystem,
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      topicName,
      settings.consumerSettings,
      akkaProducerSettings(akkaSystem),
      akkaConsumerSettings(akkaSystem),
      akkaCommitterSettings(akkaSystem))

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicDef.topicName, codec.keySerde, codec.valSerde)

  private def fs2ProducerResource(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): Resource[F, Fs2KafkaProducer[F, K, V]] =
    producerResource[F].using(fs2ProducerSettings)

  def send(k: K, v: V)(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords.one(fs2PR(k, v)))).flatten

  def send[G[+_]: Traverse](list: G[Fs2ProducerRecord[K, V]])(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords(list))).flatten

  def send(pr: Fs2ProducerRecord[K, V])(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords.one(pr))).flatten

  // APIs
  def schemaRegistry(implicit sync: Sync[F]): KafkaSchemaRegistryApi[F] =
    KafkaSchemaRegistryApi[F](this)

  def admin(
    implicit
    concurrent: Concurrent[F],
    contextShift: ContextShift[F]): KafkaTopicAdminApi[F] =
    KafkaTopicAdminApi[F, K, V](this)

  def consumerResource(implicit sync: Sync[F]): Resource[F, KafkaConsumerApi[F]] =
    KafkaConsumerApi(topicName, settings.consumerSettings)

  def monitor(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaMonitoringApi[F, K, V] = KafkaMonitoringApi[F, K, V](this)

}

final class TopicCodec[K, V] private[kafka] (val keyCodec: NJCodec[K], val valCodec: NJCodec[V]) {
  require(
    keyCodec.topicName.value === valCodec.topicName.value,
    "key and value codec should have same topic name")

  implicit val keySerde: NJSerde[K] = keyCodec.serde
  implicit val valSerde: NJSerde[V] = valCodec.serde

  val keySchema: Schema = keySerde.schema
  val valSchema: Schema = valSerde.schema

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer
}
