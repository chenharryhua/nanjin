package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Chunk, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

final class NJKafkaByteConsume[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]],
  schema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings
) extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], NJKafkaByteConsume[F]] {

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): NJKafkaByteConsume[F] =
    new NJKafkaByteConsume[F](topicName, f(consumerSettings), schema, srs)

  def resource(implicit F: Async[F]): Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
      .flatMap(_.stream)

  def assign(tps: KafkaTopicPartition[KafkaOffset])(implicit
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

  def avro(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, GenericRecord]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      stream.map { cr =>
        cr.bimap(_ => (), _ => builder.toGenericRecord(cr.record))
      }
    }

  def jackson(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Schema, String]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      stream.map { cr =>
        cr.bimap(_ => skm.consumerRecordSchema, _ => builder.toJacksonString(cr.record))
      }
    }

  def binAvro(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Schema, Chunk[Byte]]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      stream.map { cr =>
        cr.bimap(_ => skm.consumerRecordSchema, _ => builder.toBinAvro(cr.record))
      }
    }

  // assignment

  def avro(tps: KafkaTopicPartition[KafkaOffset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, GenericRecord]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      assign(tps).map { cr =>
        cr.bimap(_ => (), _ => builder.toGenericRecord(cr.record))
      }
    }

  def jackson(tps: KafkaTopicPartition[KafkaOffset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Schema, String]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      assign(tps).map { cr =>
        cr.bimap(_ => skm.consumerRecordSchema, _ => builder.toJacksonString(cr.record))
      }
    }

  def binAvro(tps: KafkaTopicPartition[KafkaOffset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Schema, Chunk[Byte]]] =
    Stream.eval(schema).flatMap { skm =>
      val builder = new PullGenericRecord(srs, topicName, skm)
      assign(tps).map { cr =>
        cr.bimap(_ => skm.consumerRecordSchema, _ => builder.toBinAvro(cr.record))
      }
    }
}

final class NJKafkaConsume[F[_], K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends UpdateConfig[ConsumerSettings[F, K, V], NJKafkaConsume[F, K, V]] {

  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): NJKafkaConsume[F, K, V] =
    new NJKafkaConsume[F, K, V](topicName, f(consumerSettings))

  def resource(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream[F, K, V](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
      .flatMap(_.stream)

  def assign(tps: KafkaTopicPartition[KafkaOffset])(implicit
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
