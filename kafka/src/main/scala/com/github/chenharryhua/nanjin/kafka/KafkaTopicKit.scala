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
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, TopicName}
import fs2.Chunk
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  ProducerRecord   => Fs2ProducerRecord,
  ProducerRecords  => Fs2ProducerRecords,
  ProducerSettings => Fs2ProducerSettings
}
import io.circe.Json
import monocle.macros.Lenses
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.collection.immutable
import scala.util.Try

@Lenses final case class KafkaTopicKit[K, V](topicDef: TopicDef[K, V], settings: KafkaSettings) {
  import topicDef.{serdeOfKey, serdeOfValue}

  val topicName: TopicName = topicDef.topicName

  def withGroupId(gid: String): KafkaTopicKit[K, V] =
    new KafkaTopicKit[K, V](topicDef, settings.withGroupId(gid))

  //need to reconstruct codec when working in spark
  @transient lazy val codec: TopicCodec[K, V] = new TopicCodec(
    serdeOfKey.asKey(settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfValue.asValue(settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def fs2ProducerSettings[F[_]: Sync]: Fs2ProducerSettings[F, K, V] =
    settings.producerSettings
      .fs2ProducerSettings[F, K, V](codec.keySerializer, codec.valueSerializer)

  def fs2ConsumerSettings[F[_]: Sync]: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
    settings.consumerSettings.fs2ConsumerSettings

  def akkaProducerSettings(akkaSystem: ActorSystem): AkkaProducerSettings[K, V] =
    settings.producerSettings.akkaProducerSettings(
      akkaSystem,
      codec.keySerializer,
      codec.valueSerializer)

  def akkaConsumerSettings(
    akkaSystem: ActorSystem): AkkaConsumerSettings[Array[Byte], Array[Byte]] =
    settings.consumerSettings.akkaConsumerSettings(akkaSystem)

  def akkaCommitterSettings(akkaSystem: ActorSystem): AkkaCommitterSettings =
    settings.consumerSettings.akkaCommitterSettings(akkaSystem)

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valueCodec)

  def toJackson[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Json =
    topicDef.toJackson(decoder(cr).record)

  def fromJackson(jsonString: String): Try[NJConsumerRecord[K, V]] =
    topicDef.fromJackson(jsonString)

  def fs2PR(key: K, value: V): Fs2ProducerRecord[K, V] =
    Fs2ProducerRecord[K, V](topicDef.topicName.value, key, value)

  def fs2ProducerRecords[P](key: K, value: V, p: P): Fs2ProducerRecords[K, V, P] =
    Fs2ProducerRecords.one[K, V, P](fs2PR(key, value), p)

  def fs2ProducerRecords(key: K, value: V): Fs2ProducerRecords[K, V, Unit] =
    Fs2ProducerRecords.one(fs2PR(key, value))

  def fs2ProducerRecords(list: Chunk[Fs2ProducerRecord[K, V]]): Fs2ProducerRecords[K, V, Unit] =
    Fs2ProducerRecords[Chunk, K, V](list)

  def fs2ProducerRecords[G[+_]: Traverse](list: G[(K, V)]): Fs2ProducerRecords[K, V, Unit] =
    Fs2ProducerRecords[G, K, V](list.map { case (k, v) => fs2PR(k, v) })

  def fs2ProducerRecords[G[+_]: Traverse, P](list: G[(K, V)], p: P): Fs2ProducerRecords[K, V, P] =
    Fs2ProducerRecords[G, K, V, P](list.map {
      case (k, v) => fs2PR(k, v)
    }, p)

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
       |${codec.valueSerde.tag}:
       |${codec.valueSerde.configProps}
       |${codec.valueSchema.toString(true)}
  """.stripMargin
  }
}

final class TopicCodec[K, V] private[kafka] (val keyCodec: NJCodec[K], val valueCodec: NJCodec[V]) {
  require(
    keyCodec.topicName === valueCodec.topicName,
    "key and value codec should have same topic name")
  val keySerde: NJSerde[K]               = keyCodec.serde
  val valueSerde: NJSerde[V]             = valueCodec.serde
  val keySchema: Schema                  = keySerde.schema
  val valueSchema: Schema                = valueSerde.schema
  val keySerializer: Serializer[K]       = keySerde.serializer
  val keyDeserializer: Deserializer[K]   = keySerde.deserializer
  val valueSerializer: Serializer[V]     = valueSerde.serializer
  val valueDeserializer: Deserializer[V] = valueSerde.deserializer
}
