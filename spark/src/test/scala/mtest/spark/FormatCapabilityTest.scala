package mtest.spark

import java.time.{Instant, LocalDate}

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedEncoder
import io.circe.shapes._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil}

object FormatCapabilityTestData {

  val schema =
    """
      |{
      |  "type": "record",
      |  "name": "Salmon",
      |  "namespace": "mtest.spark.FormatCapabilityTestData",
      |  "fields": [
      |    {
      |      "name": "localDate",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "date"
      |      }
      |    },
      |    {
      |      "name": "instant",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
      |      }
      |    },
      |    {
      |      "name": "scalaEnum",
      |      "type": {
      |        "type": "enum",
      |        "name": "Swimable",
      |        "symbols": [
      |          "No",
      |          "Yes"
      |        ]
      |      }
      |    }
      |  ]
      |}
      |
      |""".stripMargin

  object Swimable extends Enumeration {
    val No, Yes = Value
  }

  sealed trait Flyable
  case object CanFly extends Flyable
  case object CannotFly extends Flyable

  case object Atlantic
  case object Chinook
  case class Chum(size: Int)

  type Loc = Atlantic.type :+: Chinook.type :+: Chum :+: CNil

  final case class Salmon(localDate: LocalDate, instant: Instant, scalaEnum: Swimable.Value)

  val salmon = List(
    Salmon(LocalDate.now, Instant.now, Swimable.Yes),
    Salmon(LocalDate.now, Instant.now, Swimable.No),
    Salmon(LocalDate.now, Instant.now, Swimable.Yes)
  )

  implicit val te: TypedEncoder[Salmon] = shapeless.cachedImplicit

  implicit val ate: AvroTypedEncoder[Salmon] =
    AvroTypedEncoder[Salmon](AvroCodec[Salmon](schema).right.get)
}

class FormatCapabilityTest extends AnyFunSuite {
  import FormatCapabilityTestData._
  test("avro read/write identity") {
    val single = "./data/test/spark/cap/avro/single.avro"
    val multi  = "./data/test/spark/cap/avro/multi.avro"
    val rdd    = sparkSession.sparkContext.parallelize(salmon)
    ate.normalize(rdd).write.mode(SaveMode.Overwrite).json(multi)
    loaders.json[Salmon](multi).dataset.show(truncate = false)
  }

  test("jackson read/write identity") {
    val single = "./data/test/spark/cap/jackson/jackson.json"
    val multi  = "./data/test/spark/cap/jackson/multi.jackson"
  }

  test("unable to save to parquet because it doesn't support union") {
    val single = "./data/test/spark/cap/parquet/apache.parquet"
    val multi  = "./data/test/spark/cap/parquet/multi.parquet"
  }

  test("circe read/write unequal (happy failure)") {
    val single = "./data/test/spark/cap/circe/circe.json"
    val multi  = "./data/test/spark/cap/circe/multi.circe"

    val rdd = sparkSession.sparkContext.parallelize(salmon)
  }
}
