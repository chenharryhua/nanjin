package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{
  pureConsumerSettings,
  AvroSchemaPair,
  Offset,
  PullGenericRecord,
  PureConsumerSettings,
  SchemaRegistrySettings,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData

import scala.util.Try

final class KafkaByteConsume[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]],
  getSchema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings
) extends UpdateConfig[PureConsumerSettings, KafkaByteConsume[F]] {
  def properties: Map[String, String] = consumerSettings.properties

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
      .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
      .flatMap(_.stream)

  def assign(tps: TopicPartitionMap[Offset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    if (tps.isEmpty)
      Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]
    else
      KafkaConsumer
        .stream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap { c =>
          c.assign(topicName.value) *> tps.value.toList.traverse { case (tp, offset) =>
            c.seek(tp, offset.value)
          }
        }
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
      val builder = new PullGenericRecord(srs, topicName, skm)
      stream.mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => builder.toGenericRecord(cr.record)))
      }
    }

  // assignment

  def genericRecords(tps: TopicPartitionMap[Offset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      assign(tps).mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => builder.toGenericRecord(cr.record)))
      }
    }
}

final class KafkaConsume[F[_], K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends UpdateConfig[PureConsumerSettings, KafkaConsume[F, K, V]] {
  def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[PureConsumerSettings]): KafkaConsume[F, K, V] =
    new KafkaConsume[F, K, V](topicName, consumerSettings.withProperties(f(pureConsumerSettings).properties))

  def consumer(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream[F, K, V](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
      .flatMap(_.stream)

  def assign(tps: TopicPartitionMap[Offset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    if (tps.isEmpty)
      Stream.empty.covaryAll[F, CommittableConsumerRecord[F, K, V]]
    else
      KafkaConsumer
        .stream[F, K, V](consumerSettings)
        .evalTap { c =>
          c.assign(topicName.value) *> tps.value.toList.traverse { case (tp, offset) =>
            c.seek(tp, offset.value)
          }
        }
        .flatMap(_.stream)
}
