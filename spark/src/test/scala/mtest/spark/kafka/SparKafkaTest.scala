package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.connector.ConsumeByteKafka
import com.github.chenharryhua.nanjin.messages.kafka.codec.{gr2BinAvro, gr2Circe, gr2Jackson}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.sksamuel.avro4s.SchemaFor
import eu.timepit.refined.auto.*
import fs2.kafka.{AutoOffsetReset, ProducerRecord, ProducerRecords}
import io.circe.syntax.*
import io.lemonlabs.uri.typesafe.dsl.*
import monocle.syntax.all.*
import mtest.spark.sparkSession
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalDate}
object SparKafkaTestData {
  final case class Duck(f: Int, g: String)

  final case class HasDuck(a: Int, b: String, c: LocalDate, d: Instant, e: Duck)
  val duck: Duck = Duck(0, "embeded")

  val data: HasDuck =
    HasDuck(0, "a", LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli), duck)

  println(SchemaFor[HasDuck].schema)
}

class SparKafkaTest extends AnyFunSuite {
  import SparKafkaTestData.*
  implicit val ss: SparkSession = sparkSession

  val topic = TopicDef[Int, HasDuck](TopicName("duck.test"))

  val loadData: IO[Unit] =
    fs2
      .Stream(ProducerRecords(
        List(ProducerRecord(topic.topicName.value, 1, data), ProducerRecord(topic.topicName.value, 2, data))))
      .covary[IO]
      .through(ctx.produce[Int, HasDuck].updateConfig(_.withClientId("spark.kafka.test")).sink)
      .compile
      .drain

  (ctx.admin(topic.topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
    ctx.schemaRegistry.register(topic) >> loadData).unsafeRunSync()

  test("sparKafka read topic from kafka") {
    val rst = sparKafka.topic(topic).fromKafka.map(_.rdd.collect()).unsafeRunSync()
    assert(rst.toList.flatMap(_.value) === List(data, data))
  }

  test("sparKafka read topic from kafka and show minutely aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.minutely[IO]).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-hour aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.dailyHour[IO]).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-minutes aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.dailyMinute[IO]).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.daily[IO]).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show hourly aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.hourly[IO]).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show summary") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.summary[IO]("sum")).unsafeRunSync()
  }
  import sparkSession.implicits.*

  test("sparKafka someValue should filter out none values") {
    val cr1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 1, 0, 0, Nil, None, None, None, None, Some(1))
    val cr2: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 2, 0, 0, Nil, None, None, None, Some(2), None)
    val cr3: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 3, 0, 0, Nil, None, None, None, Some(3), None)
    val crs: List[NJConsumerRecord[Int, Int]] = List(cr1, cr2, cr3)
    val ds: Dataset[NJConsumerRecord[Int, Int]] = sparkSession.createDataset(crs)

    println(
      cr1.toNJProducerRecord
        .focus(_.key)
        .modify(_.map(_ + 1))
        .focus(_.value)
        .modify(_.map(_ + 1))
        .withKey(1)
        .withValue(2)
        .withTimestamp(3)
        .withPartition(4)
        .noHeaders
        .asJson
        .spaces2)

    val t =
      sparKafka
        .topic[Int, Int]("some.value")
        .crRdd(ds.rdd)
        .repartition(3)
        .descendTimestamp
        .transform(_.distinct())
    val rst = t.rdd.collect().flatMap(_.value)
    assert(rst === Seq(cr1.value.get))
  }

  test("identical json") {
    val cr1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord(
        topic = "t",
        partition = 0,
        offset = 1,
        timestamp = 0,
        timestampType = 0,
        serializedKeySize = Some(1),
        serializedValueSize = Some(2),
        key = None,
        value = Some(1),
        headers = Nil,
        leaderEpoch = Some(1)
      )
    assert(io.circe.jawn.decode[NJConsumerRecord[Int, Int]](cr1.asJson.noSpaces).toOption.get == cr1)
    val pr1 = cr1.toNJProducerRecord
    assert(io.circe.jawn.decode[NJProducerRecord[Int, Int]](pr1.asJson.noSpaces).toOption.get == pr1)
  }

  test("should be able to reproduce") {
    import fs2.Stream
    val path = "./data/test/spark/kafka/reproduce/jackson"
    val topic = TopicDef[Int, HasDuck](TopicName("duck.test"))
    sparKafka.topic(topic).fromKafka.flatMap(_.output.jackson(path).run[IO]).unsafeRunSync()

    Stream
      .eval(hadoop.filesIn(path))
      .flatMap(
        _.map(hadoop.source(_).jackson(10, topic.schemaPair.consumerSchema))
          .reduce(_ ++ _)
          .chunks
          .through(ctx.sink(topic.topicName, _.withClientId("a"))))
      .compile
      .drain
      .unsafeRunSync()
  }
  test("dump topic") {
    import io.circe.generic.auto.*
    val path = "./data/test/spark/kafka/dump/jackson"
    val p1 = path / "dump"
    val p2 = path / "download"
    sparKafka.dumpJackson("duck.test", p1).unsafeRunSync()
    sparKafka.dumpCirce(topic, p2).unsafeRunSync()
    sparKafka.upload("duck.test", p1).unsafeRunSync()
    sparKafka.sequentialUpload("duck.test", p1).unsafeRunSync()
    sparKafka.crazyUpload("duck.test", p1).unsafeRunSync()
    val s1 = sparKafka.topic[Int, HasDuck]("aa").load.jackson(p1)
    val s2 = sparKafka.topic[Int, HasDuck]("aa").load.circe(p2)
    assert(s1.cherryPick[IO](1, 1).unsafeRunSync() === s2.cherryPick[IO](1, 1).unsafeRunSync())
    assert(s1.diff(s2).rdd.count() == 0)
  }

  val duckConsume: ConsumeByteKafka[IO] =
    ctx.consume("duck.test").updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest).withGroupId("duck"))

  test("generic record") {
    val path = "./data/test/spark/kafka/consume/duck.avro"
    val sink = hadoop.sink(path).avro
    duckConsume.genericRecords
      .take(2)
      .map(_.record.value)
      .evalMap(IO.fromTry)
      .through(sink)
      .compile
      .drain
      .unsafeRunSync()
    assert(2 == sparKafka.topic(topic).load.avro(path).count[IO]("c").unsafeRunSync())
  }

  test("format") {
    duckConsume.genericRecords
      .take(2)
      .map(_.record.value)
      .evalMap(IO.fromTry)
      .map(gr => topic.consumerFormat.fromRecord(gr))
      .map(_.toNJProducerRecord)
      .map(topic.producerFormat.toRecord)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("generic record conversion") {
    duckConsume.genericRecords
      .take(2)
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(gr2Jackson(_))))
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(gr2BinAvro(_))))
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(gr2Circe(_))))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("empty") {
    val prc = sparKafka.topic(topic).emptyPrRdd.count[IO]("c").unsafeRunSync()
    val crc = sparKafka.topic(topic).emptyCrRdd.count[IO]("c").unsafeRunSync()
    assert(prc == 0)
    assert(crc == 0)
  }
}
