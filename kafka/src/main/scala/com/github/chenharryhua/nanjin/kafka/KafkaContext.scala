package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf, SerdeOfGenericRecord}
import com.sksamuel.avro4s.{AvroOutputStream, AvroOutputStreamBuilder}
import fs2.Stream
import fs2.kafka.{ConsumerSettings, Deserializer}
import io.circe.Json
import io.circe.parser.parse
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

import java.io.ByteArrayOutputStream

final class KafkaContext[F[_]](val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] with Serializable {

  override def updateConfig(f: KafkaSettings => KafkaSettings): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  def withGroupId(groupId: String): KafkaContext[F]     = updateConfig(_.withGroupId(groupId))
  def withApplicationId(appId: String): KafkaContext[F] = updateConfig(_.withApplicationId(appId))

  def asKey[K: SerdeOf]: Serde[K]   = SerdeOf[K].asKey(settings.schemaRegistrySettings.config).serde
  def asValue[V: SerdeOf]: Serde[V] = SerdeOf[V].asValue(settings.schemaRegistrySettings.config).serde

  def asKey[K](avro: NJAvroCodec[K]): Serde[K] =
    SerdeOf[K](avro).asKey(settings.schemaRegistrySettings.config).serde
  def asValue[V](avro: NJAvroCodec[V]): Serde[V] =
    SerdeOf[V](avro).asValue(settings.schemaRegistrySettings.config).serde

  def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, this)

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicNameC): KafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  def consume(topicName: TopicName)(implicit F: Sync[F]): Fs2Consume[F] =
    new Fs2Consume[F](
      topicName,
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.config)
    )
  def consume(topicName: TopicNameC)(implicit F: Sync[F]): Fs2Consume[F] =
    consume(TopicName(topicName))

  def monitor(topicName: TopicName)(implicit F: Async[F]): Stream[F, Json] = {
    val grTopic: F[KafkaTopic[F, GenericRecord, GenericRecord]] =
      new SchemaRegistryApi[F](settings.schemaRegistrySettings).kvSchema(topicName).flatMap { case (ks, vs) =>
        (ks, vs) match {
          case (Some(k), Some(v)) =>
            val ksd = SerdeOfGenericRecord(k)
            val vsd = SerdeOfGenericRecord(v)
            F.pure(TopicDef(topicName)(ksd, vsd).in(this))
          case (Some(_), None) => F.raiseError(new Exception("can not retrieve value schema from kafka"))
          case (None, Some(_)) => F.raiseError(new Exception("can not retrieve key schema from kafka"))
          case (None, None) => F.raiseError(new Exception("can not retrieve key and value schema from kafka"))
        }
      }
    Stream.eval(grTopic).flatMap { tpk =>
      val jackson: AvroOutputStreamBuilder[NJConsumerRecord[GenericRecord, GenericRecord]] =
        AvroOutputStream.json[NJConsumerRecord[GenericRecord, GenericRecord]](
          NJConsumerRecord.avroCodec(tpk.codec.keySerde.avroCodec, tpk.codec.valSerde.avroCodec).avroEncoder)
      consume(tpk.topicName).stream.map { cr =>
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream
        val writer: AvroOutputStream[NJConsumerRecord[GenericRecord, GenericRecord]] =
          jackson.to(baos).build()
        writer.write(tpk.decode(cr).toNJConsumerRecord)
        writer.close()
        parse(baos.toString())
      }.rethrow
    }
  }

  def monitor(topicName: TopicNameC)(implicit F: Async[F]): Stream[F, Json] =
    monitor(TopicName(topicName))

  def monitorK[K: SerdeOf](topicName: TopicName)(implicit F: Async[F]): Stream[F, Json] = {
    val grTopic: F[KafkaTopic[F, K, GenericRecord]] =
      new SchemaRegistryApi[F](settings.schemaRegistrySettings).kvSchema(topicName).flatMap { case (_, vs) =>
        vs match {
          case Some(v) =>
            val ksd = SerdeOf[K]
            val vsd = SerdeOfGenericRecord(v)
            F.pure(TopicDef(topicName)(ksd, vsd).in(this))
          case None => F.raiseError(new Exception("can not retrieve key and value schema from kafka"))
        }
      }
    Stream.eval(grTopic).flatMap { tpk =>
      val jackson: AvroOutputStreamBuilder[NJConsumerRecord[K, GenericRecord]] =
        AvroOutputStream.json(
          NJConsumerRecord.avroCodec(tpk.codec.keySerde.avroCodec, tpk.codec.valSerde.avroCodec).avroEncoder)
      consume(tpk.topicName).stream.map { cr =>
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream
        val writer: AvroOutputStream[NJConsumerRecord[K, GenericRecord]] =
          jackson.to(baos).build()
        writer.write(tpk.decode(cr).toNJConsumerRecord)
        writer.close()
        parse(baos.toString())
      }.rethrow
    }
  }

  def monitorK[K: SerdeOf](topicName: TopicNameC)(implicit F: Async[F]): Stream[F, Json] =
    monitorK[K](TopicName(topicName))

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): NJStateStore[K, V] =
    NJStateStore[K, V](
      storeName,
      settings.schemaRegistrySettings,
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))
  def store[K: SerdeOf, V: SerdeOf](storeName: TopicNameC): NJStateStore[K, V] =
    store[K, V](TopicName(storeName))

  def buildStreams(topology: Reader[StreamsBuilder, Unit])(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](settings.streamSettings, topology)

  def buildStreams(topology: StreamsBuilder => Unit)(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    buildStreams(Reader(topology))

  def metaData(topicName: TopicName)(implicit F: Sync[F]): F[KvSchemaMetadata] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings).metaData(topicName)

  def metaData(topicName: TopicNameC)(implicit F: Sync[F]): F[KvSchemaMetadata] =
    metaData(TopicName(topicName))
}
