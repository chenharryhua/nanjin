package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.Concurrent
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  orderingTopicPartition,
  AvroSchemaPair,
  PullGenericRecord,
  SchemaRegistrySettings,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
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
   * client
   */
  def clientR(implicit F: Async[F]): Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.stream(consumerSettings)

  /*
   * Array[Byte]
   */

  def subscribeBytes(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value))).flatMap(_.stream)
  def assignBytes(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap(_.assign(topicName.value)).flatMap(_.stream)

  /*
   * Generic Records
   */
  private def toGenericRecordStream(kc: KafkaConsumer[F, Array[Byte], Array[Byte]], pull: PullGenericRecord)(
    implicit F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    kc.partitionsMapStream.flatMap {
      _.toList.map { case (_, stream) =>
        stream.mapChunks { crs =>
          crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
        }
      }.parJoinUnbounded
    }

  def subscribe(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    for {
      skm <- Stream.eval(getSchema)
      kc <- clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      pull = new PullGenericRecord(srs, topicName, skm)
      gr <- toGenericRecordStream(kc, pull)
    } yield gr

  def assign(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    for {
      skm <- Stream.eval(getSchema)
      kc <- clientS.evalTap(_.assign(topicName.value))
      pull = new PullGenericRecord(srs, topicName, skm)
      gr <- toGenericRecordStream(kc, pull)
    } yield gr

  def assign(pos: Map[Int, Long])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] = {
    val topic_offset: Map[TopicPartition, Long] =
      pos.map { case (p, o) => new TopicPartition(topicName.value, p) -> o }

    NonEmptySet.fromSet(SortedSet.from(topic_offset.keySet)) match {
      case None                  => Stream.empty
      case Some(topic_partition) =>
        for {
          skm <- Stream.eval(getSchema)
          kc <- KafkaConsumer.stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None)).evalTap { c =>
            c.assign(topic_partition) *>
              topic_offset.toList.traverse { case (p, o) => c.seek(p, o) }
          }
          pull = new PullGenericRecord(srs, topicName, skm)
          gr <- toGenericRecordStream(kc, pull)
        } yield gr
    }
  }

  def assign(time: Instant)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    for {
      skm <- Stream.eval(getSchema)
      kc <- KafkaConsumer.stream(consumerSettings).evalTap { c =>
        for {
          _ <- c.assign(topicName.value)
          partitions <- c.partitionsFor(topicName.value)
          tps = partitions.map { pi =>
            new TopicPartition(pi.topic(), pi.partition()) -> time.toEpochMilli
          }.toMap
          tpm <- c.offsetsForTimes(tps)
          _ <- tpm.toList.traverse { case (tp, oot) =>
            oot match {
              case Some(ot) => c.seek(tp, ot.offset())
              case None     => c.seekToEnd(NonEmptyList.one(tp))
            }
          }
        } yield ()
      }
      pull = new PullGenericRecord(srs, topicName, skm)
      gr <- toGenericRecordStream(kc, pull)
    } yield gr

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
   * Circumscribed Stream
   */

  private def circumscribed(or: Either[DateTimeRange, Map[Int, (Long, Long)]])(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, Unit, Try[GenericData.Record]]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(consumerClient.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          for {
            _ <- Stream.eval(consumerClient.assign_offset_range(kc, ranges))
            schema <- Stream.eval(getSchema)
            pull = new PullGenericRecord(srs, topicName, schema)
            s <- consumerClient.circumscribed_generic_record_stream(kc, ranges, pull)
          } yield s
        }
    } yield stream

  def circumscribedStream(dateTimeRange: DateTimeRange)(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, Unit, Try[GenericData.Record]]] =
    circumscribed(Left(dateTimeRange))

  def circumscribedStream(pos: Map[Int, (Long, Long)])(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, Unit, Try[GenericData.Record]]] =
    circumscribed(Right(pos))
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
   * client
   */

  def clientR(implicit F: Async[F]): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(consumerSettings)

  /*
   * Records
   */

  def subscribe(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value))).flatMap(_.stream)

  def assign(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.assign(topicName.value)).flatMap(_.stream)

  def assign(pos: Map[Int, Long])(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] = {
    val topic_offset: Map[TopicPartition, Long] =
      pos.map { case (p, o) => new TopicPartition(topicName.value, p) -> o }

    NonEmptySet.fromSet(SortedSet.from(topic_offset.keySet)) match {
      case None      => Stream.empty
      case Some(tps) =>
        KafkaConsumer
          .stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
          .evalTap { c =>
            c.assign(tps) *>
              topic_offset.toList.traverse { case (p, o) => c.seek(p, o) }
          }
          .flatMap(_.stream)
    }
  }

  def assign(time: Instant)(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap { c =>
        for {
          _ <- c.assign(topicName.value)
          partitions <- c.partitionsFor(topicName.value)
          tps = partitions.map { pi =>
            new TopicPartition(pi.topic(), pi.partition()) -> time.toEpochMilli
          }.toMap
          tpm <- c.offsetsForTimes(tps)
          _ <- tpm.toList.traverse { case (tp, oot) =>
            oot match {
              case Some(ot) => c.seek(tp, ot.offset())
              case None     => c.seekToEnd(NonEmptyList.one(tp))
            }
          }
        } yield ()
      }
      .flatMap(_.stream)

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
   * Circumscribed Stream
   */

  private def circumscribed(or: Either[DateTimeRange, Map[Int, (Long, Long)]])(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, K, V]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(consumerClient.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          Stream.eval(consumerClient.assign_offset_range(kc, ranges)) *>
            consumerClient.circumscribed_stream(kc, ranges)
        }
    } yield stream

  def circumscribedStream(dateTimeRange: DateTimeRange)(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Left(dateTimeRange))

  def circumscribedStream(pos: Map[Int, (Long, Long)])(implicit
    F: Async[F]): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Right(pos))
}
