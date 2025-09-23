package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroFor}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import io.circe.Codec
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
    implicit val json: Codec[HasDecimal] = io.circe.generic.semiauto.deriveCodec
  }
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val codec: AvroCodec[HasDecimal] = AvroCodec[HasDecimal](schemaText)

  val topicDef: AvroTopic[Int, HasDecimal] =
    AvroTopic[Int, HasDecimal](TopicName("kafka.decimal.test"))(AvroFor[Int], AvroFor(codec))

  val now: Instant = Instant.ofEpochMilli(Instant.now.toEpochMilli)

  val data: HasDecimal =
    HasDecimal(BigDecimal(123456.001), BigDecimal(1234.5678), now)

  val expected: HasDecimal = HasDecimal(BigDecimal(123456), BigDecimal(1234.568), now)
}

class DecimalTopicTest extends AnyFunSuite {
  import DecimalTopicTestCase.*

  val topic = topicDef
  val stopic: SparKafkaTopic[IO, Int, HasDecimal] = sparKafka.topic(topicDef)

  val loadData = ctx.produce(topicDef).produce(List((1, data), (2, data)))

  (ctx
    .admin(topic.topicName.name)
    .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
    ctx.schemaRegistry.register(topic) >>
    loadData).unsafeRunSync()

  test("sparKafka kafka and spark agree on circe") {
    val path = "./data/test/spark/kafka/decimal.circe"
    stopic.fromKafka.flatMap(_.rdd.output.circe(path).run[IO]).unsafeRunSync()

    val res = stopic.load.circe(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  val enc = NJConsumerRecord.avroCodec(AvroCodec[Int], codec)

  test("sparKafka kafka and spark agree on parquet") {
    val path = "./data/test/spark/kafka/decimal.parquet"
    stopic.fromKafka.flatMap(_.rdd.out(enc).parquet(path).run[IO]).unsafeRunSync()

    val res = stopic.load(AvroCodec[Int], codec).parquet(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on jackson") {
    val path = "./data/test/spark/kafka/decimal.jackson"
    stopic.fromKafka.flatMap(_.rdd.out(enc).jackson(path).run[IO]).unsafeRunSync()

    val res = stopic.load(AvroCodec[Int], codec).jackson(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on avro") {
    val path = "./data/test/spark/kafka/decimal.avro"
    stopic.fromKafka.flatMap(_.rdd.out(enc).avro(path).run[IO]).unsafeRunSync()

    val res = stopic.load(AvroCodec[Int], codec).avro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on obj") {
    val path = "./data/test/spark/kafka/decimal.obj"
    stopic.fromKafka.flatMap(_.rdd.output.objectFile(path).run[IO]).unsafeRunSync()

    val res = stopic.load(AvroCodec[Int], codec).objectFile(path).rdd.collect().head.value.get
    assert(res == expected)
  }

  test("sparKafka kafka and spark agree on binavro") {
    val path = "./data/test/spark/kafka/decimal.bin.avro"
    stopic.fromKafka.flatMap(_.rdd.out(enc).binAvro(path).run[IO]).unsafeRunSync()

    val res = stopic.load(AvroCodec[Int], codec).binAvro(path).rdd.collect().head.value.get
    assert(res == expected)
  }

}
