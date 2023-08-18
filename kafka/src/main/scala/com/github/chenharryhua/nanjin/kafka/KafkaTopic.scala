package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamingConsumer, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaGenericDecoder, NJAvroCodec}
import com.github.chenharryhua.nanjin.messages.kafka.{
  NJConsumerMessage,
  NJConsumerRecord,
  NJConsumerRecordWithError,
  NJHeader
}
import com.sksamuel.avro4s.AvroInputStream
import fs2.Chunk
import fs2.kafka.*
import io.circe.Decoder
import io.circe.generic.auto.*
import org.apache.avro.Schema
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.ConsumerRecord as KafkaConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord as KafkaProducerRecord, RecordMetadata}
import org.apache.kafka.streams.scala.kstream.Produced

import java.io.ByteArrayInputStream
import scala.annotation.nowarn
import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  def withTopicName(tn: TopicNameC): KafkaTopic[F, K, V] =
    withTopicName(TopicName(tn))

  def withGroupId(gid: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context.withGroupId(gid))

  // need to reconstruct codec when working in spark
  @transient lazy val codec: KeyValueCodecPair[K, V] =
    topicDef.rawSerdes.register(context.settings.schemaRegistrySettings, topicName)

  @inline def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def decode[G[_, _]: NJConsumerMessage](
    gaa: G[Array[Byte], Array[Byte]]): NJConsumerRecordWithError[K, V] = {
    val cr: KafkaConsumerRecord[Array[Byte], Array[Byte]] = NJConsumerMessage[G].lens.get(gaa)
    val k: Either[String, K] =
      codec.keyCodec.tryDecode(cr.key()).toEither.leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
    val v: Either[String, V] =
      codec.valCodec.tryDecode(cr.value()).toEither.leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
    NJConsumerRecordWithError(
      partition = cr.partition,
      offset = cr.offset,
      timestamp = cr.timestamp,
      key = k,
      value = v,
      topic = cr.topic,
      timestampType = cr.timestampType.id,
      headers = cr.headers().toArray.map(h => NJHeader(h.key(), h.value())).toList
    )
  }

  val njConsumerRecordSchema: Schema =
    NJConsumerRecord.schema(codec.keySchemaFor.schema, codec.valSchemaFor.schema)

  def serializeKey(k: K): Array[Byte] = codec.keySerializer.serialize(topicName.value, k)
  def serializeVal(v: V): Array[Byte] = codec.valSerializer.serialize(topicName.value, v)

  def record(partition: Int, offset: Long)(implicit
    sync: Sync[F]): F[Option[KafkaConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset))
        .map(_.map(decoder(_).tryDecodeKeyValue)))

  // APIs

  def admin(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  // Streaming

  def asConsumer: KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](this, None, None, None)

  def asProduced: Produced[K, V] = Produced.`with`[K, V](codec.keySerde, codec.valSerde)

  def asStateStore(storeName: TopicName): NJStateStore[K, V] = {
    require(storeName.value =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](storeName, RegisteredKeyValueSerdePair(codec.keySerde, codec.valSerde))
  }
  def asStateStore(storeName: TopicNameC): NJStateStore[K, V] =
    asStateStore(TopicName(storeName))

  def produce(implicit F: Sync[F]): Fs2Produce[F, K, V] =
    new Fs2Produce[F, K, V](
      ProducerSettings[F, K, V](
        Serializer.delegate(codec.keySerializer),
        Serializer.delegate(codec.valSerializer)).withProperties(context.settings.producerSettings.config))

  // producer record
  def kafkaProducerRecord(k: K, v: V): KafkaProducerRecord[K, V] =
    new KafkaProducerRecord(topicDef.topicName.value, k, v)
  def producerRecord(k: K, v: V): ProducerRecord[K, V] =
    ProducerRecord(topicDef.topicName.value, k, v)
  def singleProducerRecords(k: K, v: V): ProducerRecords[K, V] =
    Chunk.singleton(producerRecord(k, v))

  // for testing

  def produceOne(pr: ProducerRecord[K, V])(implicit F: Async[F]): F[RecordMetadata] =
    produce.resource.use(_.produceOne_(pr).flatten)

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    produceOne(producerRecord(k, v))

  def produceCirce(
    circeStr: String)(implicit F: Async[F], @nowarn k: Decoder[K], @nowarn v: Decoder[V]): F[RecordMetadata] =
    io.circe.parser
      .decode[NJConsumerRecord[K, V]](circeStr)
      .map(_.toNJProducerRecord.noMeta.withTopicName(topicName).toProducerRecord)
      .traverse(produceOne)
      .rethrow

  def produceJackson(jacksonStr: String)(implicit F: Async[F]): F[ProducerResult[K, V]] = {
    val crCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
      NJConsumerRecord.avroCodec(codec.keySerde.avroCodec, codec.valSerde.avroCodec)
    Resource.fromAutoCloseable(F.pure(new ByteArrayInputStream(jacksonStr.getBytes))).use { is =>
      val prs: ProducerRecords[K, V] = Chunk.iterator(
        AvroInputStream
          .json[NJConsumerRecord[K, V]](crCodec.avroDecoder)
          .from(is)
          .build(crCodec.schema)
          .iterator
          .map(_.toNJProducerRecord.noMeta.withTopicName(topicName).toProducerRecord))

      produce.resource.use(_.produce(prs).flatten)
    }
  }
}
