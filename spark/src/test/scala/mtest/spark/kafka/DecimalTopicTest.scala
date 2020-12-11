package mtest.spark.kafka

import java.time.Instant
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._
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

  val ate = AvroTypedEncoder(topicDef.avroCodec)

  test("sparKafka kafka and spark agree on circe") {
    val path = "./data/test/spark/kafka/decimal.circe.json"
    sparkSession
      .alongWith(ctx)
      .topic(topicDef)
      .fromKafka
      .flatMap(_.save.circe(path).file.run(blocker))
      .unsafeRunSync

    val rdd = loaders.rdd.circe[OptionalKV[Int, HasDecimal]](path)
    val ds  = rdd.typedDataset(ate)

    rdd.save[IO].objectFile("./data/test/spark/kafka/decimal.obj").run(blocker).unsafeRunSync()
    rdd
      .save[IO](topicDef.avroCodec.avroEncoder)
      .avro("./data/test/spark/kafka/decimal.avro")
      .run(blocker)
      .unsafeRunSync()

    ds.save[IO].parquet("./data/test/spark/kafka/decimal.avro").run(blocker).unsafeRunSync()
    ds.save[IO](topicDef.avroCodec.avroEncoder)
      .jackson("./data/test/spark/kafka/decimal.jackson.json")
      .run(blocker)
      .unsafeRunSync()

    assert(rdd.collect().head.value.get == expected)
    assert(ds.collect[IO]().unsafeRunSync().head.value.get == expected)
  }

}
