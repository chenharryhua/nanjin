package mtest.kafka

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.connector.commitBatch
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import com.github.chenharryhua.nanjin.kafka.utils.genericRecord2Jackson
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, TopicDef, TopicName, TopicPartitionMap}
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ProducerRecord, ProducerRecords}
import fs2.{Chunk, Stream}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Broker-backed coverage for the concrete connector services.
  *
  * A suite fixture recreates one typed topic and one raw generic-record topic, then seeds each with a
  * baseline record. Tests exercise every producer entry point, subscription/manual-assignment modes, manual
  * commit controls, bounded stream shutdown, and `commitBatch` through public APIs. The repository's Kafka
  * tests already require the local broker and Schema Registry used by `mtest.kafka.ctx`.
  */
class ConnectorIntegrationTest extends CatsEffectSuite {

  private val TYPED_TOPIC: TopicDef[Integer, String] =
    TopicDef("connector.coverage.typed", Primitive[Integer], Primitive[String])
  private val GENERIC_TOPIC: TopicName = TopicName("connector.coverage.generic")
  private val INT_SCHEMA: Schema = Schema.create(Schema.Type.INT)
  private val GENERIC_PAIR: AvroSchemaPair =
    AvroSchemaPair(AvroSchema(INT_SCHEMA), AvroSchema(INT_SCHEMA))

  private def generic_record(key: Int, value: Int): GenericRecord = {
    val record = new Record(GENERIC_PAIR.consumerSchema)
    record.put("topic", GENERIC_TOPIC.value)
    record.put("partition", Integer.valueOf(0))
    record.put("offset", java.lang.Long.valueOf(0L))
    record.put("timestamp", java.lang.Long.valueOf(0L))
    record.put("timestampType", Integer.valueOf(0))
    record.put("headers", List.empty[Record].asJava)
    record.put("leaderEpoch", null)
    record.put("serializedKeySize", Integer.valueOf(0))
    record.put("serializedValueSize", Integer.valueOf(0))
    record.put("key", Integer.valueOf(key))
    record.put("value", Integer.valueOf(value))
    record
  }

  private def group_id(prefix: String): String = s"$prefix-${UUID.randomUUID()}"

  private def recreate_topic(name: String): IO[Unit] =
    for {
      _ <- ctx.admin(name).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt)
      _ <- IO.sleep(1.second)
      _ <- ctx.admin(name).use(_.newTopic(1, 1))
      _ <- IO.sleep(1.second)
    } yield ()

  private val setup: IO[Unit] =
    for {
      _ <- recreate_topic(TYPED_TOPIC.topicName.value)
      _ <- recreate_topic(GENERIC_TOPIC.value)
      _ <- ctx.schemaRegistry(GENERIC_TOPIC.value).delete
      _ <- ctx.produce(TYPED_TOPIC).produce(
        List(Integer.valueOf(0) -> "seed-0", Integer.valueOf(1) -> "seed-1", Integer.valueOf(2) -> "seed-2"))
      _ <- ctx
        .produceGenericRecord(GENERIC_TOPIC.value, Some(INT_SCHEMA), Some(INT_SCHEMA))
        .produceOne(generic_record(0, 0))
    } yield ()

  private val setup_fixture =
    ResourceSuiteLocalFixture("connector-integration-setup", Resource.eval(setup))

  override def munitFixtures = List(setup_fixture)

  private def first_record[K, V](
    stream: Stream[IO, TopicPartitionMap[Stream[IO, CommittableConsumerRecord[IO, K, V]]]])
    : Stream[IO, CommittableConsumerRecord[IO, K, V]] =
    stream.take(1).flatMap(partitions => Stream.emits(partitions.values).parJoinUnbounded).take(1)

  test("1.ProduceKafka supports resources, streams, every sink, batches, and both produceOne overloads") {
    val producer = ctx.produce(TYPED_TOPIC)
    val explicit = ProducerRecord(TYPED_TOPIC.topicName.value, Integer.valueOf(1), "explicit")

    for {
      _ <- producer.clientR.use(_ => IO.unit)
      clients <- producer.clientS.take(1).compile.count
      pair_results <- Stream
        .emits(List(Integer.valueOf(2) -> "pair-1", Integer.valueOf(3) -> "pair-2"))
        .covary[IO]
        .through(producer.pairSink)
        .compile
        .count
      sink_results <- Stream.emit(explicit).covary[IO].through(producer.sink).compile.count
      chunk_results <- Stream
        .emit(
          ProducerRecords(
            List(
              ProducerRecord(TYPED_TOPIC.topicName.value, Integer.valueOf(4), "chunk-1"),
              ProducerRecord(TYPED_TOPIC.topicName.value, Integer.valueOf(5), "chunk-2"))))
        .covary[IO]
        .through(producer.chunkSink)
        .compile
        .count
      batch <- producer.produce(List(Integer.valueOf(6) -> "batch-1", Integer.valueOf(7) -> "batch-2"))
      pair_metadata <- producer.produceOne(Integer.valueOf(8), "one-pair")
      record_metadata <- producer.produceOne(
        ProducerRecord(TYPED_TOPIC.topicName.value, Integer.valueOf(9), "one-record"))
    } yield {
      assertEquals(clients, 1L)
      assertEquals(pair_results, 1L)
      assertEquals(sink_results, 1L)
      assertEquals(chunk_results, 1L)
      assertEquals(batch.size, 2)
      assertEquals(pair_metadata.topic(), TYPED_TOPIC.topicName.value)
      assertEquals(record_metadata.topic(), TYPED_TOPIC.topicName.value)
    }
  }

  test("2.ProduceGenericRecord supports both sinks, produceOne, and Jackson input") {
    val producer =
      ctx.produceGenericRecord(GENERIC_TOPIC.value, Some(INT_SCHEMA), Some(INT_SCHEMA))
    val first = generic_record(1, 1)
    val second = generic_record(2, 2)
    val jackson = genericRecord2Jackson(generic_record(3, 3)).get

    for {
      sink_results <- Stream.emits(List(first, second)).covary[IO].through(producer.sink).compile.count
      chunk_results <- Stream
        .emit(Chunk(first, second))
        .covary[IO]
        .through(producer.chunkSink)
        .compile
        .count
      metadata <- producer.produceOne(generic_record(4, 4))
      jackson_metadata <- producer.jackson(jackson)
      invalid <- producer.jackson("{").attempt
    } yield {
      assertEquals(sink_results, 1L)
      assertEquals(chunk_results, 1L)
      assertEquals(metadata.topic(), GENERIC_TOPIC.value)
      assertEquals(jackson_metadata.topic(), GENERIC_TOPIC.value)
      assert(invalid.isLeft, "invalid Jackson/Avro JSON should fail")
    }
  }

  test("3.ConsumeKafka supports partition maps and all assignment modes") {
    val consumer = ctx.consume(TYPED_TOPIC).updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
    val subscribed = consumer.updateConfig(_.withGroupId(group_id("connector-partition-map")))

    for {
      partitioned <- first_record(subscribed.partitionsMapStream).timeout(10.seconds).compile.lastOrError
      assigned <- consumer.assign.take(1).timeout(10.seconds).compile.lastOrError
      by_offset <- consumer.assign(Map(0 -> 0L)).take(1).timeout(10.seconds).compile.lastOrError
      by_time <- consumer.assign(Instant.EPOCH).take(1).timeout(10.seconds).compile.lastOrError
    } yield List(partitioned, assigned, by_offset, by_time).foreach { record =>
      assertEquals(record.record.topic, TYPED_TOPIC.topicName.value)
    }
  }

  test("4.ConsumeKafka exposes manual commits and bounded-stream shutdown") {
    val manual = ctx
      .consume(TYPED_TOPIC)
      .updateConfig(
        _.withGroupId(group_id("connector-manual-typed")).withAutoOffsetReset(AutoOffsetReset.Earliest))
      .manualCommitStream
      .take(1)
      .flatMap { controlled =>
        controlled.stream.take(1).evalMap { record =>
          val offsets = Map(record.offset.topicPartition -> record.offset.offsetAndMetadata)
          controlled.commitSync.run(offsets) >> controlled.commitAsync.run(offsets)
        }
      }
      .timeout(10.seconds)
      .compile
      .drain

    val stop = ctx
      .consume(TYPED_TOPIC)
      .circumscribedStream(Map(0 -> (0L, 1L)))
      .take(1)
      .evalMap(stream => IO(assert(stream.offsets.nonEmpty)) >> stream.stopConsuming)
      .compile
      .drain

    val bounded = ctx
      .consume(TYPED_TOPIC)
      .circumscribedStream(Map(0 -> (0L, 1L)))
      .flatMap(_.stream)
      .compile
      .toList
      .map(records => assertEquals(records.size, 1))

    manual >> stop >> bounded
  }

  test("5.ConsumeGenericRecord supports partition maps, subscription, and all assignment modes") {
    val consumer = ctx
      .consumeGenericRecord(GENERIC_TOPIC.value, Some(INT_SCHEMA), Some(INT_SCHEMA))
      .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))

    for {
      partitioned <- first_record(
        consumer.updateConfig(_.withGroupId(group_id("connector-generic-map"))).partitionsMapStream)
        .timeout(10.seconds)
        .compile
        .lastOrError
      subscribed <- consumer
        .updateConfig(_.withGroupId(group_id("connector-generic-subscribe")))
        .subscribe
        .take(1)
        .timeout(10.seconds)
        .compile
        .lastOrError
      assigned <- consumer.assign.take(1).timeout(10.seconds).compile.lastOrError
      by_offset <- consumer.assign(Map(0 -> 0L)).take(1).timeout(10.seconds).compile.lastOrError
      by_time <- consumer.assign(Instant.EPOCH).take(1).timeout(10.seconds).compile.lastOrError
    } yield List(partitioned, subscribed, assigned, by_offset, by_time).foreach { record =>
      assert(record.record.value.isRight, s"generic decode failed: ${record.record.value}")
    }
  }

  test("6.ConsumeGenericRecord exposes manual commits and both bounded-stream overloads") {
    val consumer = ctx
      .consumeGenericRecord(GENERIC_TOPIC.value, Some(INT_SCHEMA), Some(INT_SCHEMA))
      .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))

    val manual = consumer
      .updateConfig(_.withGroupId(group_id("connector-manual-generic")))
      .manualCommitStream
      .take(1)
      .flatMap { controlled =>
        controlled.stream.take(1).evalMap { record =>
          val offsets: Map[TopicPartition, OffsetAndMetadata] =
            Map(record.offset.topicPartition -> record.offset.offsetAndMetadata)
          controlled.commitSync.run(offsets) >> controlled.commitAsync.run(offsets)
        }
      }
      .timeout(10.seconds)
      .compile
      .drain

    val explicit_stop = consumer
      .circumscribedStream(Map(0 -> (0L, 1L)))
      .take(1)
      .evalMap(_.stopConsuming)
      .compile
      .drain

    val time_stop = consumer
      .circumscribedStream(DateTimeRange(sydneyTime))
      .take(1)
      .evalMap(_.stopConsuming)
      .compile
      .drain

    manual >> explicit_stop >> time_stop
  }

  test("7.commitBatch commits when the requested count is reached") {
    val consumer = ctx
      .consume(TYPED_TOPIC)
      .updateConfig(
        _.withGroupId(group_id("connector-commit-batch")).withAutoOffsetReset(AutoOffsetReset.Earliest))

    consumer.subscribe
      .map(_.offset)
      .through(commitBatch[IO](n = 2, d = 100.millis))
      .take(1)
      .timeout(10.seconds)
      .compile
      .toList
      .map(result => assertEquals(result, List(2)))
  }
}
