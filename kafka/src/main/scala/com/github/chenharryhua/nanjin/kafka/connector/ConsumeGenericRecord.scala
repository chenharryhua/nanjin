package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, ReaderT}
import cats.effect.kernel.Async
import cats.syntax.bifunctor.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, TopicName, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant

/** A `ConsumerService` that reads raw Kafka bytes and decodes each record into an Avro `GenericData.Record`.
  *
  * The consumer is byte-based (`Array[Byte]` key and value). For each record it resolves the read schema
  * (reconciling any caller-supplied `schemaPair` with the schema fetched from the registry) and decodes the
  * bytes via `PullGenericRecord`. The element type is `Either[PullError, Record]`: a per-record decode
  * failure becomes a `Left(PullError)` carrying the offending key/value metadata, so one bad record does not
  * tear down the stream. The commit key is `Unit` (this connector does not project a typed key).
  *
  * It inherits all consumption modes from `ConsumerService`, subscribe, assign (by nothing, by
  * partition/offset map, or by time), manual-commit, and bounded ("circumscribed") streams, each producing
  * the same `Either[PullError, Record]` payload. Obtain an instance via
  * `KafkaContext.consumeGenericRecord(...)`.
  */
final class ConsumeGenericRecord[F[_]: Async](
  topicName: TopicName,
  schemaPair: OptionalAvroSchemaPair,
  fromSchemaRegistry: F[OptionalAvroSchemaPair],
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
) extends ConsumerService[F, Unit, Either[PullError, Record]]
    with UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeGenericRecord[F]]
    with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = consumerSettings.properties

  /** Return a copy with the underlying byte consumer settings transformed by `f`. */
  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeGenericRecord[F] =
    new ConsumeGenericRecord[F](topicName, schemaPair, fromSchemaRegistry, f(consumerSettings))

  /** The effective Avro read schema for decoded records: the registry schema reconciled with any
    * caller-supplied `schemaPair`. Evaluated against the schema registry.
    */
  lazy val schema: F[Schema] =
    fromSchemaRegistry.map(schemaPair.read(_).toSchemaPair.consumerSchema)

  /*
   * Generic Record
   */

  /** Shared decoding core for the subscribe/assign streams: fetch the schema, build a `PullGenericRecord`,
    * and per partition map each raw record to `Either[PullError, Record]` (discarding the typed key as
    * `Unit`).
    */
  private def partitions_map_stream(kc: KafkaConsumer[F, Array[Byte], Array[Byte]])
    : Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]]]] =
    Stream.eval(fromSchemaRegistry).flatMap { broker =>
      val schema = schemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      kc.partitionsMapStream.map {
        _.map { case (tp, stream) =>
          tp -> stream.mapChunks(_.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record))))
        }.toMap
      }.map(TopicPartitionMap(_))
    }

  override lazy val partitionsMapStream
    : Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(partitions_map_stream)

  /*
   * subscribe
   */

  override lazy val subscribe: Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  /*
   * assign
   */

  override lazy val assign: Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(_.assign(topicName.value))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  override def assign(partitionOffsets: Map[Int, Long])
    : Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]] =
    KafkaConsumer.stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
      .evalTap(assignByMap(_, topicName, partitionOffsets))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  override def assign(
    time: Instant): Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(assignByTime(_, topicName, time))
      .flatMap(partitions_map_stream)
      .flatMap(_.values.parJoinUnbounded)

  /*
   * manual commit stream
   */

  override lazy val manualCommitStream: Stream[F, ManualCommitStream[F, Unit, Either[PullError, Record]]] =
    Stream.eval(fromSchemaRegistry).flatMap { broker =>
      val schema = schemaPair.read(broker).toSchemaPair
      val pull: PullGenericRecord = new PullGenericRecord(schema)
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(false))
        .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
        .flatMap(kc =>
          kc.partitionsMapStream.map { pms =>
            new ManualCommitStream[F, Unit, Either[PullError, Record]] {
              override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitSync)

              override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
                ReaderT(kc.commitAsync)

              override def partitionsMapStream: TopicPartitionMap[
                Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]]] =
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
    : Stream[F, CircumscribedStream[F, Unit, Either[PullError, Record]]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topicUtils.get_offset_range(kc, topicName, or))
      isAssigned <- Stream.eval(topicUtils.assign_offset_range(kc, ranges))
      stream <-
        if isAssigned
        then
          Stream.eval(fromSchemaRegistry).map(schemaPair.read(_).toSchemaPair).flatMap { schema =>
            val pull = new PullGenericRecord(schema)
            topicUtils.circumscribed_generic_record_stream(kc, ranges, pull)
          }
        else Stream.empty
    } yield stream

  override def circumscribedStream(
    dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, Unit, Either[PullError, Record]]] =
    circumscribed(Left(dateTimeRange))

  override def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)])
    : Stream[F, CircumscribedStream[F, Unit, Either[PullError, Record]]] =
    circumscribed(Right(partitionOffsets))
}
