package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{StoreName, TopicName}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamingConsumer, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.{
  NJConsumerMessage,
  NJConsumerRecord,
  NJConsumerRecordWithError
}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaGenericDecoder, NJAvroCodec}
import com.sksamuel.avro4s.AvroInputStream
import fs2.kafka.{
  ConsumerSettings as Fs2ConsumerSettings,
  Deserializer as Fs2Deserializer,
  ProducerRecord as Fs2ProducerRecord,
  ProducerRecords as Fs2ProducerRecords,
  ProducerResult as Fs2ProducerResult,
  ProducerSettings as Fs2ProducerSettings,
  Serializer as Fs2Serializer
}
import io.circe.Decoder
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.streams.scala.kstream.Produced

import java.io.ByteArrayInputStream
import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  // need to reconstruct codec when working in spark
  @transient lazy val codec: KeyValueCodecPair[K, V] =
    topicDef.rawSerdes.register(context.settings.schemaRegistrySettings, topicName)

  @inline def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def decode[G[_, _]: NJConsumerMessage](
    gaa: G[Array[Byte], Array[Byte]]): NJConsumerRecordWithError[K, V] = {
    val cr: ConsumerRecord[Array[Byte], Array[Byte]] = NJConsumerMessage[G].lens.get(gaa)
    val k: Either[String, K] =
      codec.keyCodec.tryDecode(cr.key()).toEither.leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
    val v: Either[String, V] =
      codec.valCodec.tryDecode(cr.value()).toEither.leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
    NJConsumerRecordWithError(cr.partition, cr.offset, cr.timestamp, k, v, cr.topic, cr.timestampType.id)
  }

  def serializeKey(k: K): Array[Byte] = codec.keySerializer.serialize(topicName.value, k)
  def serializeVal(v: V): Array[Byte] = codec.valSerializer.serialize(topicName.value, v)

  def record(partition: Int, offset: Long)(implicit
    sync: Sync[F]): F[Option[ConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset))
        .map(_.map(decoder(_).tryDecodeKeyValue)))

  // APIs

  def admin(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  def monitor(implicit F: Async[F]): KafkaMonitoringApi[F, K, V] =
    KafkaMonitoringApi[F, K, V](this)

  val schemaRegistry: NJSchemaRegistry[F, K, V] = new NJSchemaRegistry[F, K, V](this)

  // Streaming

  def asConsumer: KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](this, None, None, None)

  def asProduced: Produced[K, V] = Produced.`with`[K, V](codec.keySerde, codec.valSerde)

  def asStateStore(storeName: StoreName): NJStateStore[K, V] = {
    require(storeName.value =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](storeName, RegisteredKeyValueSerdePair(codec.keySerde, codec.valSerde))
  }

  def consume(implicit F: Sync[F]): Fs2Consume[F] =
    new Fs2Consume[F](
      topicName,
      Fs2ConsumerSettings[F, Array[Byte], Array[Byte]](
        Fs2Deserializer[F, Array[Byte]],
        Fs2Deserializer[F, Array[Byte]]).withProperties(context.settings.consumerSettings.config))

  def produce(implicit F: Sync[F]): Fs2Produce[F, K, V] =
    new Fs2Produce[F, K, V](
      Fs2ProducerSettings[F, K, V](
        Fs2Serializer.delegate(codec.keySerializer),
        Fs2Serializer.delegate(codec.valSerializer)).withProperties(context.settings.producerSettings.config))

  object akka {
    import _root_.akka.actor.{ActorSystem, ClassicActorSystemProvider}
    import _root_.akka.kafka.{ConsumerSettings, ProducerSettings}
    import com.typesafe.config.Config
    def comsume(system: ActorSystem): AkkaConsume = {
      val cs = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withProperties(context.settings.consumerSettings.config)
      new AkkaConsume(topicName, cs)
    }

    def comsume(provider: ClassicActorSystemProvider): AkkaConsume = {
      val cs = ConsumerSettings(provider, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withProperties(context.settings.consumerSettings.config)
      new AkkaConsume(topicName, cs)
    }

    def comsume(cfg: Config): AkkaConsume = {
      val cs = ConsumerSettings(cfg, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withProperties(context.settings.consumerSettings.config)
      new AkkaConsume(topicName, cs)
    }

    def produce(system: ActorSystem): AkkaProduce[K, V] = {
      val ps = ProducerSettings(system, codec.keySerializer, codec.valSerializer)
        .withProperties(context.settings.producerSettings.config)
      new AkkaProduce[K, V](ps)
    }

    def produce(provider: ClassicActorSystemProvider): AkkaProduce[K, V] = {
      val ps = ProducerSettings(provider, codec.keySerializer, codec.valSerializer)
        .withProperties(context.settings.producerSettings.config)
      new AkkaProduce[K, V](ps)
    }

    def produce(cfg: Config): AkkaProduce[K, V] = {
      val ps = ProducerSettings(cfg, codec.keySerializer, codec.valSerializer)
        .withProperties(context.settings.producerSettings.config)
      new AkkaProduce[K, V](ps)
    }
  }

  def producerRecord(k: K, v: V): ProducerRecord[K, V] = new ProducerRecord(topicDef.topicName.value, k, v)
  def fs2ProducerRecord(k: K, v: V): Fs2ProducerRecord[K, V] =
    Fs2ProducerRecord(topicDef.topicName.value, k, v)

  // for testing

  def produceOne(pr: Fs2ProducerRecord[K, V])(implicit F: Async[F]): F[RecordMetadata] =
    produce.resource.use(_.produceOne_(pr).flatten)

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    produceOne(fs2ProducerRecord(k, v))

  def produceCirce(circeStr: String)(implicit F: Async[F], k: Decoder[K], v: Decoder[V]): F[RecordMetadata] =
    io.circe.parser
      .decode[NJConsumerRecord[K, V]](circeStr)
      .map(_.toNJProducerRecord.noMeta.toFs2ProducerRecord(topicName))
      .traverse(produceOne)
      .rethrow

  def produceJackson(jacksonStr: String)(implicit F: Async[F]): F[Fs2ProducerResult[K, V]] = {
    val crCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
      NJConsumerRecord.avroCodec(codec.keySerde.avroCodec, codec.valSerde.avroCodec)
    Resource.fromAutoCloseable(F.pure(new ByteArrayInputStream(jacksonStr.getBytes))).use { is =>
      val prs: List[Fs2ProducerRecord[K, V]] = AvroInputStream
        .json[NJConsumerRecord[K, V]](crCodec.avroDecoder)
        .from(is)
        .build(crCodec.schema)
        .iterator
        .map(_.toNJProducerRecord.noMeta.toFs2ProducerRecord(topicName))
        .toList

      produce.resource.use(_.produce(Fs2ProducerRecords(prs)).flatten)
    }
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
