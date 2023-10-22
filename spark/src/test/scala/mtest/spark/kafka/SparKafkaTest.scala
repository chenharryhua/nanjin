package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, NJKafkaByteConsume, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{gr2BinAvro, gr2Circe, gr2Jackson, NJAvroCodec}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.SchemaFor
import eu.timepit.refined.auto.*
import fs2.kafka.{AutoOffsetReset, ProducerRecord, ProducerRecords}
import io.circe.syntax.*
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

  val topic: KafkaTopic[IO, Int, HasDuck] = TopicDef[Int, HasDuck](TopicName("duck.test")).in(ctx)

  val loadData: IO[Unit] =
    fs2
      .Stream(ProducerRecords(
        List(ProducerRecord(topic.topicName.value, 1, data), ProducerRecord(topic.topicName.value, 2, data))))
      .covary[IO]
      .through(topic.produce.updateConfig(_.withClientId("spark.kafka.test")).pipe)
      .compile
      .drain

  (ctx.admin(topic.topicName).iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
    ctx.schemaRegistry.register(topic.topicDef) >> loadData).unsafeRunSync()

  test("sparKafka read topic from kafka") {
    val rst = sparKafka.topic(topic.topicDef).fromKafka.frdd.map(_.collect()).unsafeRunSync()
    assert(rst.toList.flatMap(_.value) === List(data, data))
  }

  test("sparKafka read topic from kafka and show minutely aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.minutely.unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-hour aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.dailyHour.unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-minutes aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.dailyMinute.unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.daily.unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show hourly aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.hourly.unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show summary") {
    sparKafka.topic(topic.topicDef).fromKafka.stats.summary.unsafeRunSync()
  }
  import sparkSession.implicits.*
  test("sparKafka should be able to bimap to other topic") {

    val src: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("src.topic")
    val d1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 1, 0, 0, None, None, None, Some(1), Nil, None)
    val d2: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 2, 0, 0, None, None, None, Some(2), Nil, None)
    val d3: NJConsumerRecord[Int, Int] = NJConsumerRecord("t", 0, 3, 0, 0, None, None, None, None, Nil, None)
    val d4: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 4, 0, 0, None, None, None, Some(4), Nil, None)
    val ds: Dataset[NJConsumerRecord[Int, Int]] = sparkSession.createDataset(List(d1, d2, d3, d4))

    val birst =
      sparKafka
        .topic(src.topicDef)
        .crRdd(IO(ds.rdd))
        .bimap(_.toString, _ + 1)(NJAvroCodec[String], NJAvroCodec[Int])
        .frdd
        .map(_.collect().toSet)
        .unsafeRunSync()
    assert(birst.flatMap(_.value) == Set(2, 3, 5))
  }

  test("sparKafka should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("src.topic")
    val d1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 1, 0, 0, None, None, None, Some(1), Nil, None)
    val d2: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 2, 0, 0, None, None, None, Some(2), Nil, None)
    val d3: NJConsumerRecord[Int, Int] = NJConsumerRecord("t", 0, 3, 0, 0, None, None, None, None, Nil, None)
    val d4: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 4, 0, 0, None, None, None, Some(4), Nil, None)
    val ds: Dataset[NJConsumerRecord[Int, Int]] = sparkSession.createDataset(List(d1, d2, d3, d4))

    val birst =
      sparKafka
        .topic(src.topicDef)
        .crRdd(IO(ds.rdd))
        .flatMap(m => m.value.map(x => m.focus(_.value).replace(Some(x - 1))))(
          NJAvroCodec[Int],
          NJAvroCodec[Int])
        .frdd
        .map(_.collect().toSet)
        .unsafeRunSync()
    assert(birst.flatMap(_.value) == Set(0, 1, 3))
  }

  test("sparKafka someValue should filter out none values") {
    val cr1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 1, 0, 0, None, None, None, Some(1), Nil, None)
    val cr2: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 2, 0, 0, None, None, Some(2), None, Nil, None)
    val cr3: NJConsumerRecord[Int, Int] =
      NJConsumerRecord("t", 0, 3, 0, 0, None, None, Some(3), None, Nil, None)
    val crs: List[NJConsumerRecord[Int, Int]]   = List(cr1, cr2, cr3)
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
        .crRdd(IO(ds.rdd))
        .repartition(3)
        .descendTimestamp
        .transform(_.distinct())
    val rst = t.frdd.map(_.collect().flatMap(_.value)).unsafeRunSync()
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
    val path  = NJPath("./data/test/spark/kafka/reproduce/jackson")
    val topic = sparKafka.topic[Int, HasDuck]("duck.test")
    topic.fromKafka.output.jackson(path).run.unsafeRunSync()

    Stream
      .eval(hadoop.filesIn(path))
      .flatMap(hadoop.jackson(topic.topic.topicDef.schemaPair.consumerSchema).source)
      .chunkN(1)
      .through(ctx.sink(topic.topicName).updateConfig(_.withClientId("a")).build)
      .compile
      .drain
      .unsafeRunSync()
  }
  test("dump topic") {
    val path = NJPath("./data/test/spark/kafka/dump/jackson")
    sparKafka.dump("duck.test", path).unsafeRunSync()
    sparKafka.upload("duck.test", path).unsafeRunSync()
    sparKafka.uploadInSequence("duck.test", path).unsafeRunSync()
  }

  val duckConsume: NJKafkaByteConsume[IO] =
    ctx.consume("duck.test").updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest).withGroupId("duck"))

  test("generic record") {
    val path = NJPath("./data/test/spark/kafka/consume/duck.avro")
    val sink = hadoop.avro(topic.topicDef.schemaPair.consumerSchema).sink(path)
    duckConsume.genericRecords.take(2).map(_.record.value).chunks.through(sink).compile.drain.unsafeRunSync()
    assert(2 == sparKafka.topic(topic).load.avro(path).count.unsafeRunSync())
  }

  test("format") {
    duckConsume.genericRecords
      .take(2)
      .map(_.record.value)
      .map(gr => topic.topicDef.consumerFormat.fromRecord(gr))
      .map(_.toNJProducerRecord)
      .map(topic.topicDef.producerFormat.toRecord)
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }

  test("generic record conversion") {
    duckConsume.genericRecords
      .take(2)
      .map(_.record.value)
      .evalTap(gr => IO.fromTry(gr2Jackson(gr)))
      .evalTap(gr => IO.fromTry(gr2BinAvro(gr)))
      .evalTap(gr => IO.fromTry(gr2Circe(gr)))
      .compile
      .drain
      .unsafeRunSync()
  }

}
