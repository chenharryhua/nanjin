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
import com.sksamuel.avro4s.{AvroInputStream, FromRecord, Record, ToRecord}
import fs2.Chunk
import fs2.kafka.*
import io.circe.Decoder
import io.circe.generic.auto.*
import org.apache.avro.generic.IndexedRecord
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

  def withSchema(pair: AvroSchemaPair): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withSchema(pair), context)

  // need to reconstruct codec when working in spark
  @transient lazy val serdePair: KeyValueSerdePair[K, V] =
    topicDef.rawSerdes.register(context.settings.schemaRegistrySettings, topicName)

  @inline def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, serdePair.key, serdePair.value)

  def decode[G[_, _]: NJConsumerMessage](
    gaa: G[Array[Byte], Array[Byte]]): NJConsumerRecordWithError[K, V] = {
    val cr: KafkaConsumerRecord[Array[Byte], Array[Byte]] = NJConsumerMessage[G].lens.get(gaa)
    val k: Either[String, K] =
      serdePair.key.tryDeserialize(cr.key()).toEither.leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
    val v: Either[String, V] =
      serdePair.value
        .tryDeserialize(cr.value())
        .toEither
        .leftMap(ex => ExceptionUtils.getRootCauseMessage(ex))
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

  private val consumerRecordCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
    NJConsumerRecord.avroCodec(topicDef.rawSerdes.key.avroCodec, topicDef.rawSerdes.value.avroCodec)

  private val toGR: ToRecord[NJConsumerRecord[K, V]]     = ToRecord(consumerRecordCodec.avroEncoder)
  private val fromGR: FromRecord[NJConsumerRecord[K, V]] = FromRecord(consumerRecordCodec.avroDecoder)

  def toRecord(nj: NJConsumerRecord[K, V]): Record          = toGR.to(nj)
  def fromRecord(gr: IndexedRecord): NJConsumerRecord[K, V] = fromGR.from(gr)

  def serializeKey(k: K): Array[Byte] = serdePair.key.serialize(k)
  def serializeVal(v: V): Array[Byte] = serdePair.value.serialize(v)

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

  def asProduced: Produced[K, V] =
    Produced.`with`[K, V](serdePair.key.serde, serdePair.value.serde)

  def asStateStore(storeName: TopicName): NJStateStore[K, V] = {
    require(storeName.value =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](storeName, RegisteredKeyValueSerdePair(serdePair.key.serde, serdePair.value.serde))
  }
  def asStateStore(storeName: TopicNameC): NJStateStore[K, V] =
    asStateStore(TopicName(storeName))

  def produce(implicit F: Sync[F]): NJKafkaProduce[F, K, V] =
    new NJKafkaProduce[F, K, V](
      ProducerSettings[F, K, V](
        Serializer.delegate(serdePair.key.serde.serializer()),
        Serializer.delegate(serdePair.value.serde.serializer()))
        .withProperties(context.settings.producerSettings.config))

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

  def produceJackson(jacksonStr: String)(implicit F: Async[F]): F[ProducerResult[K, V]] =
    Resource.fromAutoCloseable(F.pure(new ByteArrayInputStream(jacksonStr.getBytes))).use { is =>
      val prs: ProducerRecords[K, V] = Chunk.iterator(
        AvroInputStream
          .json[NJConsumerRecord[K, V]](consumerRecordCodec.avroDecoder)
          .from(is)
          .build(consumerRecordCodec.schema)
          .iterator
          .map(_.toNJProducerRecord.noMeta.withTopicName(topicName).toProducerRecord))

      produce.resource.use(_.produce(prs).flatten)
    }
}
