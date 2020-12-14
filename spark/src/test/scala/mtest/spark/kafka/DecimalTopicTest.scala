package mtest.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits._
import io.circe.generic.auto._
import mtest.spark.{blocker, contextShift, ctx, sparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal
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
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val codec: AvroCodec[HasDecimal] =
    AvroCodec[HasDecimal](schemaText).right.get

  val topicDef: TopicDef[Int, HasDecimal] =
    TopicDef[Int, HasDecimal](TopicName("kafka.decimal.test"), codec)

  val now = Instant.ofEpochMilli(Instant.now.toEpochMilli)

  val data: HasDecimal =
    HasDecimal(BigDecimal(123456.001), BigDecimal(1234.5678), now)

  val expected = HasDecimal(BigDecimal(123456), BigDecimal(1234.568), now)
}

class DecimalTopicTest extends AnyFunSuite {
  import DecimalTopicTestCase._

  val topic: KafkaTopic[IO, Int, HasDecimal] = topicDef.in(ctx)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.schemaRegister >>
    topic.send(1, data) >> topic.send(2, data)).unsafeRunSync()

  val stopic = sparkSession.alongWith(ctx).topic(topicDef)

  test("sparKafka kafka and spark agree on circe") {
    val path = "./data/test/spark/kafka/decimal.circe.json"
    stopic.fromKafka.flatMap(_.save.circe(path).file.run(blocker)).unsafeRunSync

    val rdd = stopic.load.rdd.circe(path)
    val ds  = stopic.load.circe(path)

    rdd.save.objectFile("./data/test/spark/kafka/decimal.obj").run(blocker).unsafeRunSync()
    rdd.save.avro("./data/test/spark/kafka/decimal.avro").run(blocker).unsafeRunSync()

    ds.save.parquet("./data/test/spark/kafka/decimal.avro").run(blocker).unsafeRunSync()
    ds.save.jackson("./data/test/spark/kafka/decimal.jackson.json").run(blocker).unsafeRunSync()

    assert(rdd.rdd.collect().head.value.get == expected)
    assert(ds.dataset.collect().head.value.get == expected)
  }
}
