package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, SparKafkaTopic}
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.TypedEncoder
import io.circe.Codec
//import frameless.cats.implicits._
import cats.effect.unsafe.implicits.global
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import eu.timepit.refined.auto.*

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
  import DecimalTopicTestCase.*

  val topic: KafkaTopic[IO, Int, HasDecimal]      = topicDef.in(ctx)
  val stopic: SparKafkaTopic[IO, Int, HasDecimal] = sparKafka.topic(topicDef)

  val loadData =
    stopic.prRdd(List(NJProducerRecord(1, data), NJProducerRecord(2, data))).upload.stream.compile.drain

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.schemaRegistry.register >>
    loadData).unsafeRunSync()

  test("sparKafka kafka and spark agree on circe") {
    val path = NJPath("./data/test/spark/kafka/decimal.circe.json")
    stopic.fromKafka.flatMap(_.save.circe(path).file.sink.compile.drain).unsafeRunSync()

    val run = for {
      rdd <- stopic.load.rdd.circe(path)
      ds <- stopic.load.circe(path)
    } yield {
      assert(rdd.rdd.collect().head.value.get == expected)
      assert(ds.dataset.collect().head.value.get == expected)
    }
    run.unsafeRunSync()
  }
}
