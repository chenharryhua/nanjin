package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamingConsumed, KafkaStreamingProduced}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.kafka.{ProducerRecord as Fs2ProducerRecord, ProducerResult}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.processor.RecordContext
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}

import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] = new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  // need to reconstruct codec when working in spark
  @transient lazy val codec: KeyValueCodecPair[K, V] =
    topicDef.rawSerdes.register(context.settings.schemaRegistrySettings, topicName)

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def record(partition: Int, offset: Long)(implicit sync: Sync[F]): F[Option[ConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset)).map(_.map(decoder(_).tryDecodeKeyValue)))

  // APIs

  def admin(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  def monitor(implicit F: Async[F]): KafkaMonitoringApi[F, K, V] =
    KafkaMonitoringApi[F, K, V](this)

  val schemaRegistry: NJSchemaRegistry[F, K, V] = new NJSchemaRegistry[F, K, V](this)

  // Streaming

  def asConsumer: KafkaStreamingConsumed[F, K, V] =
    new KafkaStreamingConsumed[F, K, V](this, Consumed.`with`[K, V](codec.keySerde, codec.valSerde))

  def asProducer: KafkaStreamingProduced[F, K, V] =
    new KafkaStreamingProduced[F, K, V](
      this,
      Produced.`with`[K, V](codec.keySerde, codec.valSerde),
      (_: K, _: V, _: RecordContext) => topicName.value // default extractor, can be replaced
    )

  // channels
  def fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      this,
      context.settings.producerSettings,
      context.settings.consumerSettings,
      fs2Updater.unitConsumer[F],
      fs2Updater.unitProducer[F, K, V],
      fs2Updater.unitTxnProducer[F, K, V])

  def akkaChannel(akkaSystem: ActorSystem): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      this,
      akkaSystem,
      context.settings.producerSettings,
      context.settings.consumerSettings,
      akkaUpdater.unitConsumer,
      akkaUpdater.unitProducer[K, V],
      akkaUpdater.unitCommitter)

  def producerRecord(k: K, v: V): ProducerRecord[K, V]       = new ProducerRecord(topicDef.topicName.value, k, v)
  def fs2ProducerRecord(k: K, v: V): Fs2ProducerRecord[K, V] = Fs2ProducerRecord(topicDef.topicName.value, k, v)

  // for testing

  def produceOne(pr: Fs2ProducerRecord[K, V])(implicit F: Async[F]): F[ProducerResult[K, V]] =
    fs2Channel.producer.evalMap(_.produceOne(pr)).compile.lastOrError.flatten

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[ProducerResult[K, V]] =
    produceOne(fs2ProducerRecord(k, v))
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
