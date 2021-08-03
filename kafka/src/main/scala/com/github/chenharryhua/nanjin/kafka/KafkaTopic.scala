package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.{NJStateStore, StreamingChannel}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
import org.apache.kafka.streams.state.StateSerdes

import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends TopicNameExtractor[K, V] with Serializable {

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: String): KafkaTopic[F, K, V] = new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  //need to reconstruct codec when working in spark
  @transient lazy val codec: RegisteredKeyValueSerdePair[K, V] =
    topicDef.rawSerdes.register(context.settings.schemaRegistrySettings, topicName.value)

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def record(partition: Int, offset: Long)(implicit sync: Sync[F]): F[Option[ConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset)).map(_.map(decoder(_).tryDecodeKeyValue)))

  override def toString: String = topicName.value

  // APIs

  def admin(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  def monitor(implicit F: Async[F]): KafkaMonitoringApi[F, K, V] =
    KafkaMonitoringApi[F, K, V](this)

  val schemaRegistry: NJSchemaRegistry[F, K, V] = new NJSchemaRegistry[F, K, V](this)

  // channels

  def stateSerdes: StateSerdes[K, V] = new StateSerdes[K, V](topicName.value, codec.keySerde, codec.valSerde)

  def asStateStore(name: String): NJStateStore[K, V] = {
    require(name =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](name, codec)
  }

  def kafkaStream: StreamingChannel[F, K, V] = new StreamingChannel[F, K, V](this)
  def fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      this,
      context.settings.producerSettings,
      context.settings.consumerSettings,
      fs2Updater.unitConsumer[F],
      fs2Updater.unitProducer[F, K, V])

  def akkaChannel(akkaSystem: ActorSystem): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      this,
      akkaSystem,
      context.settings.producerSettings,
      context.settings.consumerSettings,
      akkaUpdater.unitConsumer,
      akkaUpdater.unitProducer[K, V],
      akkaUpdater.unitCommitter)
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
