package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.Show
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.api._
import com.github.chenharryhua.nanjin.kafka.codec.{KafkaGenericDecoder, NJConsumerMessage}
import com.sksamuel.avro4s.Record
import io.circe.{Error, Json}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final class KafkaTopic[K, V] private[kafka] (val topicDesc: KafkaTopicDescription[K, V])
    extends TopicNameExtractor[K, V] with Serializable {
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
  def fs2Channel[F[_]: ConcurrentEffect: ContextShift: Timer]: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDesc.topicDef.topicName,
      topicDesc.settings.producerSettings
        .fs2ProducerSettings(topicDesc.codec.keySerializer, topicDesc.codec.valueSerializer),
      topicDesc.settings.consumerSettings.fs2ConsumerSettings)

  def akkaResource[F[_]: ConcurrentEffect: ContextShift](
    akkaSystem: ActorSystem): Resource[F, KafkaChannels.AkkaChannel[F, K, V]] =
    Resource.make(
      ConcurrentEffect[F].delay(
        new KafkaChannels.AkkaChannel[F, K, V](
          topicDesc.topicDef.topicName,
          topicDesc.settings.producerSettings.akkaProducerSettings(
            akkaSystem,
            topicDesc.codec.keySerializer,
            topicDesc.codec.valueSerializer),
          topicDesc.settings.consumerSettings.akkaConsumerSettings(akkaSystem),
          topicDesc.settings.consumerSettings.akkaCommitterSettings(akkaSystem))))(_ =>
      ConcurrentEffect[F].unit)

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](
      topicDesc.topicDef.topicName,
      topicDesc.codec.keySerde,
      topicDesc.codec.valueSerde)

  // APIs
  def schemaRegistry[F[_]: Sync]: KafkaSchemaRegistryApi[F] = api.KafkaSchemaRegistryApi[F](this.topicDesc)

  def admin[F[_]: Concurrent: ContextShift]: KafkaTopicAdminApi[F] =
    api.KafkaTopicAdminApi[F, K, V](this)

  def consumer[F[_]: Sync]: KafkaConsumerApi[F, K, V] = api.KafkaConsumerApi[F, K, V](this.topicDesc)

  def producer[F[_]: ConcurrentEffect]: KafkaProducerApi[F, K, V] =
    api.KafkaProducerApi[F, K, V](this.topicDesc)

  def monitor[F[_]: ConcurrentEffect: ContextShift: Timer]: KafkaMonitoringApi[F, K, V] =
    api.KafkaMonitoringApi[F, K, V](this, topicDesc.settings.rootPath)
}

object KafkaTopic {
  implicit def showKafkaTopic[K, V]: Show[KafkaTopic[K, V]] = _.topicName
}
