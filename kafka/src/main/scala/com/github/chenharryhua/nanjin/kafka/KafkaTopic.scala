package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.kafka.{
  CommitterSettings,
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings
}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.kafka.{ProducerSettings, ConsumerSettings => Fs2ConsumerSettings}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val context: KafkaContext[F],
  akkaUpdater: AkkaSettingsUpdater[K, V],
  fs2Updater: Fs2SettingsUpdater[F, K, V])
    extends TopicNameExtractor[K, V] with KafkaTopicProducer[F, K, V] with Serializable {
  import topicDef.{serdeOfKey, serdeOfVal}

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context, akkaUpdater, fs2Updater)

  def updateAkkaConsumerSettings(
    f: AkkaConsumerSettings[Array[Byte], Array[Byte]] => AkkaConsumerSettings[Array[Byte], Array[Byte]])
    : KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context, akkaUpdater.updateConsumer(f), fs2Updater)

  def updateAkkaProducerSettings(f: AkkaProducerSettings[K, V] => AkkaProducerSettings[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context, akkaUpdater.updateProducer(f), fs2Updater)

  def updateAkkaCommitterSettings(f: CommitterSettings => CommitterSettings): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context, akkaUpdater.updateCommitter(f), fs2Updater)

  def updateFs2ConsumerSettings(
    f: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] => Fs2ConsumerSettings[F, Array[Byte], Array[Byte]])
    : KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context, akkaUpdater, fs2Updater.updateConsumer(f))

  def updateFs2ProducerSettings(f: ProducerSettings[F, K, V] => ProducerSettings[F, K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context, akkaUpdater, fs2Updater.updateProducer(f))

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  //need to reconstruct codec when working in spark
  @transient lazy val codec: KafkaTopicCodec[K, V] = new KafkaTopicCodec(
    serdeOfKey.asKey(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName.value),
    serdeOfVal.asValue(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName.value)
  )

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def record(partition: Int, offset: Long)(implicit sync: Sync[F]): F[Option[ConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset)).map(_.map(decoder(_).tryDecodeKeyValue)))

  override def toString: String = topicName.value

  def show: String = {
    import cats.derived.auto.show._
    s"""
       |topic: $topicName
       |consumer-group-id: ${context.settings.groupId}
       |stream-app-id:     ${context.settings.appId}
       |settings:
       |  ${context.settings.consumerSettings.show}
       |  ${context.settings.producerSettings.show}
       |  ${context.settings.schemaRegistrySettings.show}
       |  ${context.settings.adminSettings.show}
       |  ${context.settings.streamSettings.show}
       |
       |  ${codec.keyCodec.cfg.tag}:
       |  ${codec.keyCodec.cfg.configProps}
       |  ${codec.keySchemaFor.schema.toString(true)}
       |
       |  ${codec.valCodec.cfg.tag}:
       |  ${codec.valCodec.cfg.configProps}
       |  ${codec.valSchemaFor.schema.toString(true)}
   """.stripMargin
  }

  // APIs

  def admin(implicit concurrent: Concurrent[F], contextShift: ContextShift[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  def monitor(implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaMonitoringApi[F, K, V] = KafkaMonitoringApi[F, K, V](this)

  val schemaRegistry: NJSchemaRegistry[F, K, V] = new NJSchemaRegistry[F, K, V](this)

  // channels
  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicDef.topicName, codec.keySerde, codec.valSerde)

  def fs2Channel(implicit F: Sync[F]): KafkaChannels.Fs2Channel[F, K, V] = {
    import fs2.kafka.{ConsumerSettings, Deserializer, ProducerSettings, Serializer}
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDef.topicName,
      fs2Updater.producer.run(
        ProducerSettings[F, K, V](Serializer.delegate(codec.keySerializer), Serializer.delegate(codec.valSerializer))
          .withProperties(context.settings.producerSettings.config)),
      fs2Updater.consumer.run(
        ConsumerSettings[F, Array[Byte], Array[Byte]](Deserializer[F, Array[Byte]], Deserializer[F, Array[Byte]])
          .withProperties(context.settings.consumerSettings.config)))
  }

  def akkaChannel(akkaSystem: ActorSystem): KafkaChannels.AkkaChannel[F, K, V] = {
    import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}
    new KafkaChannels.AkkaChannel[F, K, V](
      topicName,
      akkaUpdater.producer.run(
        ProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer)
          .withProperties(context.settings.producerSettings.config)),
      akkaUpdater.consumer.run(
        ConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
          .withProperties(context.settings.consumerSettings.config)),
      akkaUpdater.committer.run(CommitterSettings(akkaSystem)),
      akkaSystem)
  }
}

final class NJSchemaRegistry[F[_], K, V](kt: KafkaTopic[F, K, V]) extends Serializable {

  def register(implicit F: Sync[F]): F[(Option[Int], Option[Int])] =
    new SchemaRegistryApi[F](kt.context.settings.schemaRegistrySettings)
      .register(kt.topicName, kt.topicDef.schemaForKey.schema, kt.topicDef.schemaForVal.schema)

  def delete(implicit F: Sync[F]): F[(List[Integer], List[Integer])] =
    new SchemaRegistryApi[F](kt.context.settings.schemaRegistrySettings).delete(kt.topicName)

  def testCompatibility(implicit F: Sync[F]): F[CompatibilityTestReport] =
    new SchemaRegistryApi[F](kt.context.settings.schemaRegistrySettings)
      .testCompatibility(kt.topicName, kt.topicDef.schemaForKey.schema, kt.topicDef.schemaForVal.schema)

}
