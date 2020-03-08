package com.github.chenharryhua.nanjin.kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.{
  ProducerMessage,
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  Deserializer     => Fs2Deserializer,
  ProducerRecord   => Fs2ProducerRecord,
  ProducerSettings => Fs2ProducerSettings,
  Serializer       => Fs2Serializer
}
import io.circe.Json
import monocle.macros.Lenses
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, Serializer}

import scala.collection.immutable
import scala.util.Try

@Lenses final case class KafkaTopicKit[K, V](topicDef: TopicDef[K, V], settings: KafkaSettings) {
  import topicDef.{serdeOfKey, serdeOfVal}

  val topicName: TopicName = topicDef.topicName

  def withGroupId(gid: String): KafkaTopicKit[K, V] =
    new KafkaTopicKit[K, V](topicDef, settings.withGroupId(gid))

  //need to reconstruct codec when working in spark
  @transient lazy val codec: TopicCodec[K, V] = new TopicCodec(
    serdeOfKey.asKey(settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfVal.asValue(settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def fs2ProducerSettings[F[_]: Sync]: Fs2ProducerSettings[F, K, V] =
    Fs2ProducerSettings[F, K, V](
      Fs2Serializer.delegate(codec.keySerializer),
      Fs2Serializer.delegate(codec.valSerializer)).withProperties(settings.producerSettings.config)

  def fs2ConsumerSettings[F[_]: Sync]: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
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
  """.stripMargin
  }
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
