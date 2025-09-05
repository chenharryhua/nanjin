package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  orderTopicPartition,
  AvroSchemaPair,
  Offset,
  OffsetRange,
  PullGenericRecord,
  SchemaRegistrySettings,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.SortedSet
import scala.util.Try

final class ConsumeByteKafka[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]],
  getSchema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings
) extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeByteKafka[F]]
    with HasProperties {
  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeByteKafka[F] =
    new ConsumeByteKafka[F](topicName, f(consumerSettings), getSchema, srs)

  /*
   * consume
   */
  def clientR(implicit F: Async[F]): Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.stream(consumerSettings)

  /** raw bytes from kafka, un-deserialized
    * @return
    *   bytes
    */
  def subscribe(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value))).flatMap(_.stream)

  def assign(partition: Int, offset: Long)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap { kc =>
      val tp = new TopicPartition(topicName.value, partition)
      kc.assign(NonEmptySet.one(tp)) *> kc.seek(tp, offset)
    }.flatMap(_.stream)

  def assign(partitions: List[Int])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    partitions match {
      case head :: rest =>
        val nes = NonEmptySet(head, SortedSet.from(rest))
        clientS.evalTap(_.assign(topicName.value, nes)).flatMap(_.stream)
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

  def empty: Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]]

  /*
   * manual commit stream
   */

  def manualCommitStream(implicit
    F: Async[F]): Stream[F, ManualCommitStream[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
        .flatMap(kc =>
          kc.partitionsMapStream.map { pms =>
            new ManualCommitStream[F, Unit, Try[GenericData.Record]] {
              override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitSync)

              override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitAsync)

              override def partitionsMapStream: Map[
                TopicPartition,
                Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]]] =
                TopicPartitionMap(pms)
                  .mapValues(_.mapChunks { crs =>
                    crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
                  })
                  .value
            }
          })
    }

  /*
   * range
   */

  def range(dateTimeRange: DateTimeRange)(implicit
    F: Async[F]): Stream[F, RangedStream[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalMap { kc =>
          for {
            tpm <- consumerClient.get_offset_range(kc, topicName, dateTimeRange)
            _ <- consumerClient.assign_range(kc, tpm)
          } yield (kc, tpm)
        }
        .flatMap { case (kc, tpm) => consumerClient.ranged_gr_stream(kc, tpm, pull) }
    }

  def range(pos: Map[Int, (Long, Long)])(implicit
    F: Async[F]): Stream[F, RangedStream[F, Unit, Try[GenericData.Record]]] = {
    val mor: Map[TopicPartition, Option[OffsetRange]] =
      pos.map { case (partition, (start, end)) =>
        new TopicPartition(topicName.value, partition) -> OffsetRange(Offset(start), Offset(end))
      }
    val tps = TopicPartitionMap(mor).flatten
    Stream.eval(getSchema).flatMap { skm =>
      val pull = new PullGenericRecord(srs, topicName, skm)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalMap(kc => consumerClient.assign_range(kc, tps).as((kc, tps)))
        .flatMap { case (kc, tpm) => consumerClient.ranged_gr_stream(kc, tpm, pull) }
    }
  }
}

final class ConsumeKafka[F[_], K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends UpdateConfig[ConsumerSettings[F, K, V], ConsumeKafka[F, K, V]] with HasProperties {
  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](topicName, f(consumerSettings))

  /*
   * consume
   */

  def clientR(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(consumerSettings)

  def subscribe(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value))).flatMap(_.stream)

  def empty: Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.empty.covaryAll[F, CommittableConsumerRecord[F, K, V]]

  /*
   * manual commit stream
   */

  def manualCommitStream(implicit F: Async[F]): Stream[F, ManualCommitStream[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(kc =>
        kc.partitionsMapStream.map { pms =>
          new ManualCommitStream[F, K, V] {
            override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitSync)

            override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitAsync)

            override def partitionsMapStream
              : Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]] =
              pms
          }
        })

  /*
   * range
   */

  def range(dateTimeRange: DateTimeRange)(implicit F: Async[F]): Stream[F, RangedStream[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalMap { kc =>
        for {
          tpm <- consumerClient.get_offset_range(kc, topicName, dateTimeRange)
          _ <- consumerClient.assign_range(kc, tpm)
        } yield (kc, tpm)
      }
      .flatMap { case (kc, tpm) => consumerClient.ranged_stream(kc, tpm) }

  def range(pos: Map[Int, (Long, Long)])(implicit F: Async[F]): Stream[F, RangedStream[F, K, V]] = {
    val or: Map[TopicPartition, Option[OffsetRange]] =
      pos.map { case (partition, (start, end)) =>
        new TopicPartition(topicName.value, partition) -> OffsetRange(Offset(start), Offset(end))
      }
    val tps = TopicPartitionMap(or).flatten
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalMap(kc => consumerClient.assign_range(kc, tps).as((kc, tps)))
      .flatMap { case (kc, tpm) => consumerClient.ranged_stream(kc, tpm) }
  }
}
