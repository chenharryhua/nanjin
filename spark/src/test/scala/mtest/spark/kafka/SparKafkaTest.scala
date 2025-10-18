package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.kafka.connector.ConsumeGenericRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{
  genericRecord2BinAvro,
  genericRecord2Circe,
  genericRecord2Jackson
}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.sksamuel.avro4s.SchemaFor
import eu.timepit.refined.auto.*
import fs2.kafka.AutoOffsetReset
import io.circe.syntax.*
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
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

  val topic = AvroTopic[Int, HasDuck](TopicName("duck.test"))

  val loadData: IO[Unit] =
    fs2.Stream
      .emits[IO, (Int, HasDuck)](
        List((1, data), (2, null), (null.asInstanceOf[Int], data), (null.asInstanceOf[Int], null)))
      .covary[IO]
      .chunks
      .through(ctx.produce[Int, HasDuck](topic).updateConfig(_.withClientId("spark.kafka.test")).sink)
      .compile
      .drain

  (ctx
    .admin(topic.topicName.name)
    .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
    ctx.schemaRegistry.register(topic) >> loadData).unsafeRunSync()

  test("identical json") {
    val cr1: NJConsumerRecord[Int, Int] =
      NJConsumerRecord(
        topic = "t",
        partition = 0,
        offset = 1,
        timestamp = 0,
        timestampType = 0,
        serializedKeySize = 1,
        serializedValueSize = 2,
        key = None,
        value = Some(1),
        headers = Nil,
        leaderEpoch = Some(1)
      )
    assert(io.circe.jawn.decode[NJConsumerRecord[Int, Int]](cr1.asJson.noSpaces).toOption.get == cr1)
    val pr1 = cr1.toNJProducerRecord
    assert(io.circe.jawn.decode[NJProducerRecord[Int, Int]](pr1.asJson.noSpaces).toOption.get == pr1)
  }

  val duckConsume: ConsumeGenericRecord[IO, Int, HasDuck] =
    ctx
      .consumeGenericRecord(topic)
      .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest).withGroupId("duck"))

  test("generic record") {
    val path = "./data/test/spark/kafka/consume/duck.avro"
    val sink = hadoop.sink(path).avro
    duckConsume.subscribe
      .take(4)
      .map(_.record.value)
      .evalMap(IO.fromTry)
      .through(sink)
      .compile
      .drain
      .unsafeRunSync()
    val List(a, b, c, d) = sparkSession.loadRdd[NJConsumerRecord[Int, HasDuck]](path).avro.collect().toList
    assert(a.key.get == 1)
    assert(a.value.get == data)
    assert(b.key.get == 2)
    assert(b.value.isEmpty)
    assert(c.key.get == 0)
    assert(c.value.get == data)
    assert(d.key.get == 0)
    assert(d.value.isEmpty)
  }

  test("upload/download") {
    val path = "./data/test/spark/kafka/dump"
    val run = sparKafka.dump(topic, path) >> sparKafka.upload(topic, path)

    run.unsafeRunSync()
  }

  test("generic record conversion") {
    duckConsume.subscribe
      .take(4)
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(genericRecord2Jackson(_))))
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(genericRecord2BinAvro(_))))
      .evalTap(gr => IO.fromTry(gr.record.value.flatMap(genericRecord2Circe(_))))
      .compile
      .drain
      .unsafeRunSync()
  }
}
