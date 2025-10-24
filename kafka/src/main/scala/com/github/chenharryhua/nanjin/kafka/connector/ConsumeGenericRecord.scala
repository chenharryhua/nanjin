package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  orderingTopicPartition,
  AvroTopic,
  OptionalAvroSchemaPair,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.SortedSet

final class ConsumeGenericRecord[F[_]: Async, K, V](
  avroTopic: AvroTopic[K, V],
  getSchema: F[OptionalAvroSchemaPair],
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
) extends ConsumerService[F, Unit, Either[PullGenericRecordException, GenericData.Record]]
    with UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeGenericRecord[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(
    f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeGenericRecord[F, K, V] =
    new ConsumeGenericRecord[F, K, V](avroTopic, getSchema, f(consumerSettings))

  def schema: F[Schema] =
    getSchema.map(avroTopic.pair.optionalSchemaPair.read(_).toSchemaPair.consumerSchema)

  /*
   * Generic Record
   */

  private def toGenericRecordStream(kc: KafkaConsumer[F, Array[Byte], Array[Byte]])
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { broker =>
      val schema = avroTopic.pair.optionalSchemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      kc.partitionsMapStream.flatMap {
        _.toList.map { case (_, stream) =>
          stream.mapChunks { crs =>
            crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
          }
        }.parJoinUnbounded
      }
    }

  /*
   * subscribe
   */

  def subscribe
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(avroTopic.topicName.name.value)))
      .flatMap(toGenericRecordStream)

  /*
   * assign
   */

  def assign
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.assign(avroTopic.topicName.name.value))
      .flatMap(toGenericRecordStream)

  def assign(partitionOffsets: Map[Int, Long]): Stream[
    F,
    CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] = {
    val start_offsets: Map[TopicPartition, Long] =
      partitionOffsets.map { case (p, o) => new TopicPartition(avroTopic.topicName.name.value, p) -> o }

    NonEmptySet.fromSet(SortedSet.from(start_offsets.keySet)) match {
      case None                  => Stream.empty
      case Some(topic_partition) =>
        KafkaConsumer
          .stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
          .evalTap { c =>
            c.assign(topic_partition) *>
              start_offsets.toList.traverse { case (p, o) => c.seek(p, o) }
          }
          .flatMap(toGenericRecordStream)
    }
  }

  def assign(time: Instant)
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap { c =>
        for {
          _ <- c.assign(avroTopic.topicName.name.value)
          partitions <- c.partitionsFor(avroTopic.topicName.name.value)
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
      .flatMap(toGenericRecordStream)

  /*
   * manual commit stream
   */

  def manualCommitStream
    : Stream[F, ManualCommitStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { broker =>
      val schema = avroTopic.pair.optionalSchemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalTap(_.subscribe(NonEmptyList.one(avroTopic.topicName.name.value)))
        .flatMap(kc =>
          kc.partitionsMapStream.map { pms =>
            new ManualCommitStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]] {
              override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitSync)

              override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitAsync)

              override def partitionsMapStream: Map[
                TopicPartition,
                Stream[
                  F,
                  CommittableConsumerRecord[
                    F,
                    Unit,
                    Either[PullGenericRecordException, GenericData.Record]]]] =
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

  private def circumscribed(or: Either[DateTimeRange, Map[Int, (Long, Long)]])
    : Stream[F, CircumscribedStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topic_utils.get_offset_range(kc, avroTopic.topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          for {
            _ <- Stream.eval(topic_utils.assign_offset_range(kc, ranges))
            schema <- Stream.eval(getSchema).map(avroTopic.pair.optionalSchemaPair.read(_).toSchemaPair)
            pull = new PullGenericRecord(schema)
            s <- topic_utils.circumscribed_generic_record_stream(kc, ranges, pull)
          } yield s
        }
    } yield stream

  def circumscribedStream(dateTimeRange: DateTimeRange)
    : Stream[F, CircumscribedStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    circumscribed(Left(dateTimeRange))

  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)])
    : Stream[F, CircumscribedStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    circumscribed(Right(partitionOffsets))
}
