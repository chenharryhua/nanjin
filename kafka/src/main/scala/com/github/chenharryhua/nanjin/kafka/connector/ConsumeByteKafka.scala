package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.{Async, Concurrent}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{orderingTopicPartition, AvroSchemaPair, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.SortedSet
import scala.util.Try

final class ConsumeByteKafka[F[_]](
  topicName: TopicName,
  getSchema: F[AvroSchemaPair],
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
) extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeByteKafka[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeByteKafka[F] =
    new ConsumeByteKafka[F](topicName, getSchema, f(consumerSettings))

  /*
   * Array[Byte]
   */

  def subscribeBytes(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(_.stream)
  def assignBytes(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.stream(consumerSettings).evalTap(_.assign(topicName.value)).flatMap(_.stream)

  /*
   * Generic Record
   */

  private def toGenericRecordStream(kc: KafkaConsumer[F, Array[Byte], Array[Byte]])(implicit
    F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { schema =>
      val pull: PullGenericRecord = new PullGenericRecord(topicName, schema)
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

  def subscribe(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(toGenericRecordStream)

  /*
   * assign
   */

  def assign(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
    KafkaConsumer.stream(consumerSettings).evalTap(_.assign(topicName.value)).flatMap(toGenericRecordStream)

  def assign(pos: Map[Int, Long])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] = {
    val start_offsets: Map[TopicPartition, Long] =
      pos.map { case (p, o) => new TopicPartition(topicName.value, p) -> o }

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

  def assign(time: Instant)(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Unit, Try[GenericData.Record]]] =
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
      .flatMap(toGenericRecordStream)

  /*
   * manual commit stream
   */

  def manualCommitStream(implicit
    F: Async[F]): Stream[F, ManualCommitStream[F, Unit, Try[GenericData.Record]]] =
    Stream.eval(getSchema).flatMap { schema =>
      val pull: PullGenericRecord = new PullGenericRecord(topicName, schema)
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
      ranges <- Stream.eval(utils.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          for {
            _ <- Stream.eval(utils.assign_offset_range(kc, ranges))
            schema <- Stream.eval(getSchema)
            pull: PullGenericRecord = new PullGenericRecord(topicName, schema)
            s <- utils.circumscribed_generic_record_stream(kc, ranges, pull)
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
