package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.{Show, Traverse}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import com.github.chenharryhua.nanjin.kafka.api._
import com.github.chenharryhua.nanjin.kafka.codec.{KafkaGenericDecoder, NJConsumerMessage}
import com.sksamuel.avro4s.Record
import fs2.kafka.{KafkaProducer, ProducerResult}
import io.circe.{Error, Json}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
import cats.implicits._

final class KafkaTopic[F[_]: ConcurrentEffect: ContextShift: Timer, K, V] private[kafka] (
  val topicDesc: KafkaTopicDescription[K, V])
    extends TopicNameExtractor[K, V] {
  import topicDesc.topicDef.{showKey, showValue}

  val topicName: String = topicDesc.topicDef.topicName

  val consumerGroupId: Option[KafkaConsumerGroupId] = topicDesc.consumerGroupId

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    topicDesc.decoder(cr)

  def toAvro[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Record =
    topicDesc.toAvro[G](cr)

  def fromAvro(cr: Record): NJConsumerRecord[K, V] = topicDesc.fromAvro(cr)

  def toJson[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Json =
    topicDesc.toJson[G](cr)

  def fromJsonStr(jsonString: String): Either[Error, NJConsumerRecord[K, V]] =
    topicDesc.fromJsonStr(jsonString)

  //channels
  def fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    KafkaChannels.Fs2Channel[F, K, V](
      topicDesc.topicDef.topicName,
      topicDesc.fs2ProducerSettings,
      topicDesc.fs2ConsumerSettings)

  def akkaResource(akkaSystem: ActorSystem): Resource[F, KafkaChannels.AkkaChannel[F, K, V]] =
    Resource.make(
      ConcurrentEffect[F].delay(KafkaChannels.AkkaChannel[F, K, V](
        topicDesc.topicDef.topicName,
        topicDesc.akkaProducerSettings(akkaSystem),
        topicDesc.akkaConsumerSettings(akkaSystem),
        topicDesc.akkaCommitterSettings(akkaSystem)
      )))(_ => ConcurrentEffect[F].unit)

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    KafkaChannels.StreamingChannel[K, V](
      topicDesc.topicDef.topicName,
      topicDesc.codec.keySerde,
      topicDesc.codec.valueSerde)

  private val pr: Resource[F, KafkaProducer[F, K, V]] =
    fs2.kafka.producerResource[F].using(topicDesc.fs2ProducerSettings)

  def send(k: K, v: V): F[ProducerResult[K, V, Unit]] =
    pr.use(_.produce(topicDesc.fs2ProducerRecords(k, v))).flatten

  def send[G[+_]: Traverse](list: G[(K, V)]): F[ProducerResult[K, V, Unit]] =
    pr.use(_.produce(topicDesc.fs2ProducerRecords(list))).flatten

  // APIs
  def schemaRegistry: KafkaSchemaRegistryApi[F] = api.KafkaSchemaRegistryApi[F](this.topicDesc)
  def admin: KafkaTopicAdminApi[F]              = api.KafkaTopicAdminApi[F, K, V](this.topicDesc)
  def consumer: KafkaConsumerApi[F, K, V]       = api.KafkaConsumerApi[F, K, V](this.topicDesc)

  def monitor: KafkaMonitoringApi[F, K, V] =
    api.KafkaMonitoringApi[F, K, V](this, topicDesc.settings.rootPath)
}

object KafkaTopic {
  implicit def showKafkaTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.topicName
}
