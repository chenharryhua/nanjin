package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import frameless.TypedEncoder
import io.circe.Codec
//import frameless.cats.implicits._
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.auto.*
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
object DecimalTopicTestCase {

  val schemaText: String =
    """
      |
      |{
      |  "type": "record",
      |  "name": "HasDecimal",
      |  "namespace": "mtest.spark.kafka.DecimalTopicTestCase",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 6,
      |        "scale": 0
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 7,
      |        "scale": 3
      |      }
      |    },
      |    {
      |      "name": "c",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  final case class HasDecimal(a: BigDecimal, b: BigDecimal, c: Instant)

  object HasDecimal {

    implicit val teHasDecimal: TypedEncoder[HasDecimal] = shapeless.cachedImplicit
    implicit val json: Codec[HasDecimal]                = io.circe.generic.semiauto.deriveCodec
  }
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val codec: AvroCodec[HasDecimal] = AvroCodec[HasDecimal](schemaText)

  val topicDef: TopicDef[Int, HasDecimal] =
    TopicDef[Int, HasDecimal](TopicName("kafka.decimal.test"), codec)

  val now: Instant = Instant.ofEpochMilli(Instant.now.toEpochMilli)

  val data: HasDecimal =
    HasDecimal(BigDecimal(123456.001), BigDecimal(1234.5678), now)

  val expected: HasDecimal = HasDecimal(BigDecimal(123456), BigDecimal(1234.568), now)
}

class DecimalTopicTest extends AnyFunSuite {
  import DecimalTopicTestCase.*

  val topic: KafkaTopic[IO, Int, HasDecimal]      = ctx.topic(topicDef)
  val stopic: SparKafkaTopic[IO, Int, HasDecimal] = sparKafka.topic(topicDef)

  val loadData: IO[Unit] =
    stopic
      .prRdd(List(NJProducerRecord(stopic.topicName, 1, data), NJProducerRecord(stopic.topicName, 2, data)))
      .producerRecords[IO](100)
      .through(ctx.produce(topicDef.rawSerdes).sink)
      .compile
      .drain

  (ctx.admin(topic.topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
    ctx.schemaRegistry.register(topic.topicDef) >>
    loadData).unsafeRunSync()

  test("sparKafka kafka and spark agree on circe") {
    val path = "./data/test/spark/kafka/decimal.circe"
    stopic.fromKafka.flatMap(_.output.circe(path).run[IO]).unsafeRunSync()

    val res = stopic.load.circe(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on parquet") {
    val path = "./data/test/spark/kafka/decimal.parquet"
    stopic.fromKafka.flatMap(_.output.parquet(path).run[IO]).unsafeRunSync()

    val res = stopic.load.parquet(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on jackson") {
    val path = "./data/test/spark/kafka/decimal.jackson"
    stopic.fromKafka.flatMap(_.output.jackson(path).run[IO]).unsafeRunSync()

    val res = stopic.load.jackson(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on avro") {
    val path = "./data/test/spark/kafka/decimal.avro"
    stopic.fromKafka.flatMap(_.output.avro(path).run[IO]).unsafeRunSync()

    val res = stopic.load.avro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on obj") {
    val path = "./data/test/spark/kafka/decimal.obj"
    stopic.fromKafka.flatMap(_.output.objectFile(path).run[IO]).unsafeRunSync()

    val res = stopic.load.objectFile(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on binavro") {
    val path = "./data/test/spark/kafka/decimal.bin.avro"
    stopic.fromKafka.flatMap(_.output.binAvro(path).run[IO]).unsafeRunSync()

    val res = stopic.load.binAvro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

}
