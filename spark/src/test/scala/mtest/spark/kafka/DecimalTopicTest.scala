package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.TypedEncoder
import io.circe.Codec
//import frameless.cats.implicits._
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.auto.*
import io.circe.generic.auto.*
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

  val codec: NJAvroCodec[HasDecimal] =
    NJAvroCodec[HasDecimal](schemaText).right.get

  val topicDef: TopicDef[Int, HasDecimal] =
    TopicDef[Int, HasDecimal](TopicName("kafka.decimal.test"), codec)

  val now = Instant.ofEpochMilli(Instant.now.toEpochMilli)

  val data: HasDecimal =
    HasDecimal(BigDecimal(123456.001), BigDecimal(1234.5678), now)

  val expected = HasDecimal(BigDecimal(123456), BigDecimal(1234.568), now)
}

class DecimalTopicTest extends AnyFunSuite {
  import DecimalTopicTestCase.*

  val topic: KafkaTopic[IO, Int, HasDecimal]      = topicDef.in(ctx)
  val stopic: SparKafkaTopic[IO, Int, HasDecimal] = sparKafka.topic(topicDef)

  val loadData =
    stopic
      .prRdd(List(NJProducerRecord(1, data), NJProducerRecord(2, data)))
      .producerRecords(stopic.topicName, 100)
      .through(stopic.topic.produce.pipe)
      .compile
      .drain

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
    topic.schemaRegistry.register >>
    loadData).unsafeRunSync()

  test("sparKafka kafka and spark agree on circe") {
    val path = NJPath("./data/test/spark/kafka/decimal.circe")
    stopic.fromKafka.flatMap(_.output.circe(path).run).unsafeRunSync()

    val res = stopic.load.circe(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on parquet") {
    val path = NJPath("./data/test/spark/kafka/decimal.parquet")
    stopic.fromKafka.flatMap(_.output.parquet(path).run).unsafeRunSync()

    val res = stopic.load.parquet(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on jackson") {
    val path = NJPath("./data/test/spark/kafka/decimal.jackson")
    stopic.fromKafka.flatMap(_.output.jackson(path).run).unsafeRunSync()

    val res = stopic.load.jackson(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on avro") {
    val path = NJPath("./data/test/spark/kafka/decimal.avro")
    stopic.fromKafka.flatMap(_.output.avro(path).run).unsafeRunSync()

    val res = stopic.load.avro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on obj") {
    val path = NJPath("./data/test/spark/kafka/decimal.obj")
    stopic.fromKafka.flatMap(_.output.objectFile(path).run).unsafeRunSync()

    val res = stopic.load.objectFile(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on binavro") {
    val path = NJPath("./data/test/spark/kafka/decimal.bin.avro")
    stopic.fromKafka.flatMap(_.output.binAvro(path).run).unsafeRunSync()

    val res = stopic.load.binAvro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

}
