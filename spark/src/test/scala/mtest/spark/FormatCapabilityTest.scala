package mtest.spark

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}
import io.circe.shapes._
import io.circe.generic.auto._
import frameless.TypedEncoder
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec

object FormatCapabilityTestData {

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

  final case class Salmon(
    localDate: LocalDate,
    localDateTime: LocalDateTime,
    instant: Instant,
    coprod: Loc,
    scalaEnum: Swimable.Value)

  val salmon = List(
    Salmon(LocalDate.now, LocalDateTime.now, Instant.now, Coproduct[Loc](Atlantic), Swimable.Yes),
    Salmon(LocalDate.now, LocalDateTime.now, Instant.now, Coproduct[Loc](Chinook), Swimable.No),
    Salmon(LocalDate.now, LocalDateTime.now, Instant.now, Coproduct[Loc](Chum(100)), Swimable.Yes)
  )
  // implicit val ate : AvroTypedEncoder[Salmon] = new AvroTypedEncoder(TypedEncoder[Salmon], NJAvroCodec[Salmon])
}

class FormatCapabilityTest extends AnyFunSuite {
  import FormatCapabilityTestData._
  test("avro read/write identity") {
    val single = "./data/test/spark/cap/avro/single.avro"
    val multi  = "./data/test/spark/cap/avro/multi.avro"
    val rdd    = sparkSession.sparkContext.parallelize(salmon)
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
