package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamingConsumer, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
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
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.ConsumerRecord as KafkaConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.streams.scala.kstream.Produced

import java.io.ByteArrayInputStream
import scala.annotation.nowarn

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  def withTopicName(tn: TopicNameL): KafkaTopic[F, K, V] =
    withTopicName(TopicName(tn))

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

  def serializeKey(k: K): Array[Byte] = serdePair.key.serialize(k)
  def serializeVal(v: V): Array[Byte] = serdePair.value.serialize(v)

  // consumer and producer

  def consume(implicit F: Sync[F]): NJKafkaConsume[F, K, V] =
    new NJKafkaConsume[F, K, V](
      topicName,
      ConsumerSettings[F, K, V](
        Deserializer.delegate[F, K](serdePair.key.serde.deserializer()),
        Deserializer.delegate[F, V](serdePair.value.serde.deserializer()))
        .withProperties(context.settings.consumerSettings.properties)
    )

  def produce(implicit F: Sync[F]): NJKafkaProduce[F, K, V] =
    new NJKafkaProduce[F, K, V](
      ProducerSettings[F, K, V](
        Serializer.delegate(serdePair.key.serde.serializer()),
        Serializer.delegate(serdePair.value.serde.serializer()))
        .withProperties(context.settings.producerSettings.properties))

  // Streaming

  def asConsumer: KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](this, None, None, None)

  def asProduced: Produced[K, V] =
    Produced.`with`[K, V](serdePair.key.serde, serdePair.value.serde)

  def asStateStore(storeName: TopicName): NJStateStore[K, V] = {
    require(storeName.value =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](storeName, KeyValueSerdePair(serdePair.key, serdePair.value))
  }
  def asStateStore(storeName: TopicNameL): NJStateStore[K, V] =
    asStateStore(TopicName(storeName))

  // for testing

  def produceOne(pr: ProducerRecord[K, V])(implicit F: Async[F]): F[RecordMetadata] =
    produce.resource.use(_.produceOne_(pr).flatten)

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    produceOne(ProducerRecord(topicName.value, k, v))

  /** Generate a Producer Record from a Consumer Record encoded in circe
    *
    * @param circeStr
    *   circe string
    * @return
    */
  def produceCirce(
    circeStr: String)(implicit F: Async[F], @nowarn k: Decoder[K], @nowarn v: Decoder[V]): F[RecordMetadata] =
    io.circe.parser
      .decode[NJConsumerRecord[K, V]](circeStr)
      .map(_.toNJProducerRecord.noMeta.withTopicName(topicName).toProducerRecord)
      .traverse(produceOne)
      .rethrow

  /** Generate a Producer Record from a Consumer Record encoded in jackson
    *
    * @param jacksonStr
    *   jackson string
    * @return
    */
  def produceJackson(jacksonStr: String)(implicit F: Async[F]): F[ProducerResult[K, V]] =
    Resource.fromAutoCloseable(F.pure(new ByteArrayInputStream(jacksonStr.getBytes))).use { is =>
      val prs: ProducerRecords[K, V] = Chunk.iterator(
        AvroInputStream
          .json[NJConsumerRecord[K, V]](topicDef.consumerCodec)
          .from(is)
          .build(topicDef.schemaPair.consumerSchema)
          .iterator
          .map(_.toNJProducerRecord.noMeta.withTopicName(topicName).toProducerRecord))

      produce.resource.use(_.produce(prs).flatten)
    }
}
