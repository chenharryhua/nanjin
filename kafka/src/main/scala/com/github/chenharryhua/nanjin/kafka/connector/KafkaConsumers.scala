package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  orderTopicPartition,
  pureConsumerSettings,
  AvroSchemaPair,
  PullGenericRecord,
  SchemaRegistrySettings,
  TransientConsumer
}
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.SortedSet
import scala.util.Try

final class KafkaByteConsume[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]],
  getSchema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings
) extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], KafkaByteConsume[F]]
    with HasProperties {
  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): KafkaByteConsume[F] =
    new KafkaByteConsume[F](topicName, f(consumerSettings), getSchema, srs)

  /*
   * consume
   */
  def consumer(implicit F: Async[F]): Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  /** raw bytes from kafka, un-deserialized
    * @return
    *   bytes
    */
  def subscribe(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
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

  def assign(partitions: List[Int])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    partitions match {
      case head :: rest =>
        val nes = NonEmptySet(head, SortedSet.from(rest))
        KafkaConsumer
          .stream[F, Array[Byte], Array[Byte]](consumerSettings)
          .evalTap(_.assign(topicName.value, nes))
          .flatMap(_.stream)
      case Nil => Stream.empty
    }

  def assign(partition: Int)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    assign(List(partition))

  /** Retrieve Generic.Record from kafka
    *
    * @return
    *   avro GenericData.Record instance of NJConsumerRecord
    */
  def genericRecords(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      subscribe.mapChunks { crs =>
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

  def genericRecords(partitions: List[Int])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      assign(partitions).mapChunks { crs =>
        crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
      }
    }

  def genericRecords(partition: Int)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    genericRecords(List(partition))

  def empty: Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]]

  def range(partition_offsets: Map[Int, (Long, Long)])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      partition_offsets.map { case (partition, (from, to)) =>
        assign(partition, from)
          .takeWhile(ccr => ccr.record.offset < to, takeFailure = true)
          .mapChunks { crs =>
            crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
          }
          .filterNot(_.record.offset > to)
      }.foldLeft(empty)(_.merge(_))
    }

  def range(dateRange: DateTimeRange)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] = {
    val run: F[Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]]] =
      TransientConsumer(topicName, pureConsumerSettings.withProperties(consumerSettings.properties))
        .offsetRangeFor(dateRange)
        .map(_.flatten.value)
        .map { tm =>
          val partition_offsets = tm.map { case (tp, rng) =>
            tp.partition() -> (rng.from.value, rng.until.value - 1)
          }
          range(partition_offsets)
        }
    Stream.force(run).timeoutOnPull(timeout)
  }
}

final class KafkaConsume[F[_], K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends UpdateConfig[ConsumerSettings[F, K, V], KafkaConsume[F, K, V]] with HasProperties {
  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): KafkaConsume[F, K, V] =
    new KafkaConsume[F, K, V](topicName, f(consumerSettings))

  /*
   * consume
   */

  def consumer(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def subscribe(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
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

  def assign(partitions: List[Int])(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    partitions match {
      case head :: rest =>
        val nes = NonEmptySet(head, SortedSet.from(rest))
        KafkaConsumer
          .stream[F, K, V](consumerSettings)
          .evalTap(_.assign(topicName.value, nes))
          .flatMap(_.stream)
      case Nil => Stream.empty
    }

  def assign(partition: Int)(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    assign(List(partition))

  def empty: Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.empty.covaryAll[F, CommittableConsumerRecord[F, K, V]]

  def range(partition_offsets: Map[Int, (Long, Long)])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    partition_offsets.map { case (partition, (from, to)) =>
      assign(partition, from)
        .takeWhile(_.record.offset < to, takeFailure = true)
        .filterNot(_.record.offset > to)
    }.foldLeft(empty)(_.merge(_))

  def range(dateRange: DateTimeRange)(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] = {
    val run: F[Stream[F, CommittableConsumerRecord[F, K, V]]] =
      TransientConsumer(topicName, pureConsumerSettings.withProperties(consumerSettings.properties))
        .offsetRangeFor(dateRange)
        .map(_.flatten.value)
        .map { tm =>
          val partition_offsets = tm.map { case (tp, rng) =>
            tp.partition() -> (rng.from.value, rng.until.value - 1)
          }
          range(partition_offsets)
        }
    Stream.force(run).timeoutOnPull(timeout)
  }
}
