package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.Async
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.bifunctor.toBifunctorOps
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, TopicName, TopicPartitionMap, given}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.SortedSet

final class ConsumeGenericRecord[F[_]: Async](
  topicName: TopicName,
  schemaPair: OptionalAvroSchemaPair,
  fromSchemaRegistry: F[OptionalAvroSchemaPair],
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
) extends ConsumerService[F, Unit, Either[PullException, Record]]
    with UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeGenericRecord[F]]
    with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeGenericRecord[F] =
    new ConsumeGenericRecord[F](topicName, schemaPair, fromSchemaRegistry, f(consumerSettings))

  lazy val schema: F[Schema] =
    fromSchemaRegistry.map(schemaPair.read(_).toSchemaPair.consumerSchema)

  /*
   * Generic Record
   */

  private def partitions_map_stream(kc: KafkaConsumer[F, Array[Byte], Array[Byte]]): Stream[
    F,
    TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]]]] =
    Stream.eval(fromSchemaRegistry).flatMap { broker =>
      val schema = schemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      kc.partitionsMapStream.map {
        _.map { case (tp, stream) =>
          tp -> stream.mapChunks(_.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record))))
        }.toMap
      }.map(TopicPartitionMap(_))
    }

  override lazy val partitionsMapStream: Stream[
    F,
    TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(partitions_map_stream)

  /*
   * subscribe
   */

  override lazy val subscribe: Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  /*
   * assign
   */

  override lazy val assign: Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.assign(topicName.value))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  override def assign(partitionOffsets: Map[Int, Long])
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]] = {
    val start_offsets: Map[TopicPartition, Long] =
      partitionOffsets.map { case (p, o) => new TopicPartition(topicName.value, p) -> o }

    NonEmptySet.fromSet(SortedSet.from(start_offsets.keySet)) match {
      case None                  => Stream.empty
      case Some(topic_partition) =>
        KafkaConsumer
          .stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
          .evalTap { c =>
            c.assign(topic_partition) *>
              start_offsets.toList.traverse { case (p, o) => c.seek(p, o) }
          }
          .flatMap(partitions_map_stream)
          .flatMap(_.values.parJoinUnbounded)
    }
  }

  override def assign(
    time: Instant): Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]] =
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
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  /*
   * manual commit stream
   */

  override lazy val manualCommitStream
    : Stream[F, ManualCommitStream[F, Unit, Either[PullException, Record]]] =
    Stream.eval(fromSchemaRegistry).flatMap { broker =>
      val schema = schemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
        .flatMap(kc =>
          kc.partitionsMapStream.map { pms =>
            new ManualCommitStream[F, Unit, Either[PullException, Record]] {
              override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitSync)

              override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitAsync)

              override def partitionsMapStream: TopicPartitionMap[
                Stream[F, CommittableConsumerRecord[F, Unit, Either[PullException, Record]]]] =
                TopicPartitionMap(pms)
                  .mapValues(_.mapChunks {
                    _.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
                  })
            }
          })
    }

  /*
   * Circumscribed Stream
   */

  private def circumscribed(or: Either[DateTimeRange, Map[Int, (Long, Long)]])
    : Stream[F, CircumscribedStream[F, Unit, Either[PullException, Record]]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topicUtils.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          for {
            _ <- Stream.eval(topicUtils.assign_offset_range(kc, ranges))
            schema <- Stream.eval(fromSchemaRegistry).map(schemaPair.read(_).toSchemaPair)
            pull = new PullGenericRecord(schema)
            s <- topicUtils.circumscribed_generic_record_stream(kc, ranges, pull)
          } yield s
        }
    } yield stream

  override def circumscribedStream(
    dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, Unit, Either[PullException, Record]]] =
    circumscribed(Left(dateTimeRange))

  override def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)])
    : Stream[F, CircumscribedStream[F, Unit, Either[PullException, Record]]] =
    circumscribed(Right(partitionOffsets))
}
