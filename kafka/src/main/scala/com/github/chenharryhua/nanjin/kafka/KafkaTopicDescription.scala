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
import cats.{Show, Traverse}
import com.github.chenharryhua.nanjin.kafka.codec._
import com.sksamuel.avro4s.Record
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  ProducerRecord   => Fs2ProducerRecord,
  ProducerRecords  => Fs2ProducerRecords,
  ProducerSettings => Fs2ProducerSettings
}
import io.circe.{Error, Json}
import monocle.function.At
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable

@Lenses final case class KafkaTopicDescription[K, V](
  topicDef: TopicDef[K, V],
  settings: KafkaSettings) {
  import topicDef.{serdeOfKey, serdeOfValue}

  def consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.config
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(settings.consumerSettings)
      .map(KafkaConsumerGroupId)

  @transient lazy val codec: TopicCodec[K, V] = TopicCodec(
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

  def toAvro[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Record =
    topicDef.toAvro(decoder(cr).record)

  def fromAvro(cr: Record): NJConsumerRecord[K, V] = topicDef.fromAvro(cr)

  def toJson[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Json =
    topicDef.toJson(decoder(cr).record)

  def fromJsonStr(jsonString: String): Either[Error, NJConsumerRecord[K, V]] =
    topicDef.fromJson(jsonString)

  def fs2ProducerRecords[P](key: K, value: V, p: P): Fs2ProducerRecords[K, V, P] =
    Fs2ProducerRecords.one[K, V, P](Fs2ProducerRecord[K, V](topicDef.topicName, key, value), p)

  def fs2ProducerRecords(key: K, value: V): Fs2ProducerRecords[K, V, Unit] =
    Fs2ProducerRecords.one(Fs2ProducerRecord[K, V](topicDef.topicName, key, value))

  def fs2ProducerRecords[G[+_]: Traverse](list: G[(K, V)]): Fs2ProducerRecords[K, V, Unit] =
    Fs2ProducerRecords[G, K, V](list.map {
      case (k, v) => Fs2ProducerRecord[K, V](topicDef.topicName, k, v)
    })

  def fs2ProducerRecords[G[+_]: Traverse, P](list: G[(K, V)], p: P): Fs2ProducerRecords[K, V, P] =
    Fs2ProducerRecords[G, K, V, P](list.map {
      case (k, v) => Fs2ProducerRecord[K, V](topicDef.topicName, k, v)
    }, p)

  def akkaProducerRecords(key: K, value: V): ProducerMessage.Envelope[K, V, NotUsed] =
    ProducerMessage.single[K, V](new ProducerRecord[K, V](topicDef.topicName, key, value))

  def akkaProducerRecords[P](key: K, value: V, p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.single[K, V, P](new ProducerRecord[K, V](topicDef.topicName, key, value), p)

  def akkaProducerRecord(seq: immutable.Seq[(K, V)]): ProducerMessage.Envelope[K, V, NotUsed] =
    ProducerMessage.multi(seq.map { case (k, v) => new ProducerRecord(topicDef.topicName, k, v) })

  def akkaProducerRecord[P](seq: immutable.Seq[(K, V)], p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.multi(
      seq.map { case (k, v) => new ProducerRecord(topicDef.topicName, k, v) },
      p)

  def show: String =
    s"""
       |topic: ${topicDef.show}
       |settings: 
       |${settings.show}
       |key-schema: 
       |${codec.keySchema.toString(true)}
       |value-schema:
       |${codec.valueSchema.toString(true)}
  """.stripMargin
}

object KafkaTopicDescription {
  implicit def showKafkaTopicData[K, V]: Show[KafkaTopicDescription[K, V]] = _.show
}
