package mtest.spark.kafka

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset

class SparKafkaTest extends AnyFunSuite {
  val embed = EmbeddedForTaskSerializable(0, "embeded")
  val data  = ForTaskSerializable(0, "a", LocalDate.now, Instant.now, embed)
  val topic = ctx.topic[Int, ForTaskSerializable]("serializable.test")

  (topic.admin.idefinitelyWantToDeleteTheTopic >> topic.schemaRegistry.register >>
    topic.send(List(0 -> data, 1 -> data))).unsafeRunSync()

  test("read topic from kafka") {
    val rst =
      topic.kit.sparKafka.fromKafka[IO].flatMap(_.values.collect[IO]()).unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("save topic to disk in Jackson format") {
    topic.kit.sparKafka
      .withParamUpdate(_.withOverwrite.withJackson)
      .fromKafka[IO]
      .map(_.save())
      .unsafeRunSync
  }

  test("read topic from disk in Jackson format") {
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withJackson)
      .fromDisk[IO]
      .values
      .collect[IO]()
      .unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("save topic to disk in json format") {
    topic.kit.sparKafka
      .withParamUpdate(_.withOverwrite.withJson)
      .fromKafka[IO]
      .map(_.save())
      .unsafeRunSync
  }

  test("read topic from disk in json format") {
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withJson)
      .fromDisk[IO]
      .values
      .collect[IO]()
      .unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("save topic to disk in parquet format") {
    topic.kit.sparKafka
      .withParamUpdate(_.withOverwrite.withParquet)
      .fromKafka[IO]
      .map(_.save())
      .unsafeRunSync
  }

  test("read topic from disk in parquet format") {
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withParquet)
      .fromDisk[IO]
      .values
      .collect[IO]()
      .unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("save topic to disk in avro format") {
    topic.kit.sparKafka
      .withParamUpdate(_.withOverwrite.withAvro)
      .fromKafka[IO]
      .map(_.save())
      .unsafeRunSync
  }

  test("read topic from disk in avro format") {
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withAvro)
      .fromDisk[IO]
      .values
      .collect[IO]()
      .unsafeRunSync
    assert(rst.toList === List(data, data))
  }

  test("replay") {
    topic.kit.sparKafka
      .withParamUpdate(
        _.withConversionTactics(_.withoutPartition.withoutTimestamp).withJson.withOverwrite)
      .replay[IO]
      .unsafeRunSync
  }

  test("read topic from kafka and show aggragation result") {
    topic.kit.sparKafka.fromKafka[IO].flatMap(_.stats.minutely).unsafeRunSync
  }

  test("read topic from kafka and show json") {
    val tpk = TopicDef[String, trip_record](
      "nyc_yellow_taxi_trip_data",
      ManualAvroSchema[trip_record](trip_record.schema)).in(ctx)

    tpk.kit.sparKafka
      .fromKafka[IO, String](_.asJson.noSpaces)
      .flatMap(_.show[IO](truncate = false, numRows = 1))
      .unsafeRunSync
  }

  test("should be able to be transformed to other topic") {
    val target = ctx.topic[Int, Int]("from.serializable.test")
    val prepare =
      (target.admin.idefinitelyWantToDeleteTheTopic >> target.schemaRegistry.register) >>
        (topic.kit.sparKafka
          .fromKafka[IO]
          .flatMap(_.toProducerRecords.transform(target.kit)(identity, _.a).upload.compile.drain))
    prepare.unsafeRunSync
    val src =
      topic.kit.sparKafka.fromKafka[IO].flatMap(_.values.collect[IO]()).unsafeRunSync.map(_.a)
    val tgt = target.kit.sparKafka.fromKafka[IO].flatMap(_.values.collect[IO]()).unsafeRunSync
    assert(src === tgt)
  }

  test("someValue should filter out none values") {
    val cr1: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 0, 0, None, Some(1), "t", 0)
    val cr2: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 0, 0, Some(1), None, "t", 0)
    val crs: List[NJConsumerRecord[Int, Int]]        = List(cr1, cr2)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(crs)

    val t   = TopicDef[Int, Int]("some.value").in(ctx).kit.sparKafka.crDataset(ds)
    val rst = t.someValues.typedDataset.collect[IO]().unsafeRunSync()
    assert(rst === Seq(cr1))
  }
}
