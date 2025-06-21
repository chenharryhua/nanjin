package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.{
  orderTopicPartition,
  pureConsumerSettings,
  AvroSchemaPair,
  PullGenericRecord,
  PureConsumerSettings,
  SchemaRegistrySettings
}
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition

import scala.util.Try

final class KafkaByteConsume[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]],
  getSchema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings
) extends UpdateConfig[PureConsumerSettings, KafkaByteConsume[F]] with HasProperties {

  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[PureConsumerSettings]): KafkaByteConsume[F] =
    new KafkaByteConsume[F](
      topicName,
      consumerSettings.withProperties(f(pureConsumerSettings).properties),
      getSchema,
      srs)

  def consumer(implicit F: Async[F]): Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  /** raw bytes from kafka, un-deserialized
    * @return
    *   bytes
    */
  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(_.stream)

  def assign(partition: Int, offset: Long)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap { kc =>
        val tp = new TopicPartition(topicName.value, partition)
        kc.assign(NonEmptySet.one(tp)) *> kc.seek(tp, offset)
      }
      .flatMap(_.stream)

  def assign(partition: Int)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.assign(NonEmptySet.one(new TopicPartition(topicName.value, partition))))
      .flatMap(_.stream)

  /** Retrieve Generic.Record from kafka
    *
    * @return
    *
    * an avro GenericData.Record instance of NJConsumerRecord
    */
  def genericRecords(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      stream.mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
      }
    }

  // assignment

  def genericRecords(partition: Int, offset: Long)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      assign(partition, offset).mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
      }
    }

  def genericRecords(partition: Int)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      assign(partition).mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
      }
    }
}

final class KafkaConsume[F[_], K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends UpdateConfig[PureConsumerSettings, KafkaConsume[F, K, V]] with HasProperties {
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[PureConsumerSettings]): KafkaConsume[F, K, V] =
    new KafkaConsume[F, K, V](topicName, consumerSettings.withProperties(f(pureConsumerSettings).properties))

  def consumer(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream[F, K, V](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(_.stream)

  def assign(partition: Int, offset: Long)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream[F, K, V](consumerSettings)
      .evalTap { kc =>
        val tp = new TopicPartition(topicName.value, partition)
        kc.assign(NonEmptySet.one(tp)) *> kc.seek(tp, offset)
      }
      .flatMap(_.stream)

  def assign(partition: Int)(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream[F, K, V](consumerSettings)
      .evalTap(_.assign(NonEmptySet.one(new TopicPartition(topicName.value, partition))))
      .flatMap(_.stream)
}
