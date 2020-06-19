package mtest.spark.kafka

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.WithAvroSchema
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.TypedDataset
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

class SparKafkaTest extends AnyFunSuite {
  val embed = EmbeddedForTaskSerializable(0, "embeded")

  val data = ForTaskSerializable(
    0,
    "a",
    LocalDate.now,
    Instant.ofEpochMilli(Instant.now.toEpochMilli),
    embed)
  val topic = ctx.topic[Int, ForTaskSerializable](TopicName("serializable.test"))

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> topic.schemaRegister >>
    topic.send(List(topic.fs2PR(0, data), topic.fs2PR(1, data)))).unsafeRunSync()

  test("read topic from kafka") {
    val rst =
      topic.sparKafka.fromKafka.flatMap(_.crDataset.values.collect[IO]()).unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("read topic from kafka and show aggragation result") {
    topic.sparKafka.fromKafka.flatMap(_.stats.minutely).unsafeRunSync
  }

  test("read topic from kafka and show json") {
    val tpk = TopicDef[String, trip_record](
      TopicName("nyc_yellow_taxi_trip_data"),
      WithAvroSchema[trip_record](trip_record.schema)).in(ctx)

    tpk.sparKafka.fromKafka.flatMap(_.crDataset.show).unsafeRunSync
  }

  test("should be able to bimap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int](TopicName("src.topic"))
    val tgt: KafkaTopic[IO, String, Int]             = ctx.topic[String, Int](TopicName("target.topic"))
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst =
      src.sparKafka
        .crDataset(ds)
        .bimapTo(tgt)(_.toString, _ + 1)
        .values
        .collect[IO]()
        .unsafeRunSync
        .toSet
    assert(birst == Set(2, 3, 5))
  }

  test("should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int](TopicName("src.topic"))
    val tgt: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int](TopicName("target.topic"))
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst =
      src.sparKafka
        .crDataset(ds)
        .flatMapTo(tgt)(m => m.value.map(x => NJConsumerRecord.value.set(Some(x - 1))(m)))
        .values
        .collect[IO]()
        .unsafeRunSync
        .toSet
    assert(birst == Set(0, 1, 3))
  }

  test("someValue should filter out none values") {
    val cr1: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val cr2: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 2, 0, Some(2), None, "t", 0)
    val cr3: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 3, 0, Some(3), None, "t", 0)
    val crs: List[NJConsumerRecord[Int, Int]]        = List(cr1, cr2, cr3)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(crs)

    val t   = TopicDef[Int, Int](TopicName("some.value")).in(ctx).sparKafka.crDataset(ds)
    val rst = t.someValues.typedDataset.collect[IO]().unsafeRunSync()
    assert(rst === Seq(cr1))
  }
}
