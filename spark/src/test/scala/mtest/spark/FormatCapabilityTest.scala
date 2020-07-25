package mtest.spark

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}
import io.circe.shapes._
import io.circe.generic.auto._

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
}

class FormatCapabilityTest extends AnyFunSuite {
  import FormatCapabilityTestData._
  test("avro read/write identity") {
    val single = "./data/test/spark/cap/avro/single.avro"
    val multi  = "./data/test/spark/cap/avro/multi.avro"
    val rdd    = sparkSession.sparkContext.parallelize(salmon)
    val prepare = fileSink[IO](blocker).delete(single) >>
      rdd.single[IO](blocker).avro(single) >>
      fileSink[IO](blocker).delete(multi) >>
      rdd.multi[IO](blocker).avro(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.avro[Salmon](single).collect().toSet == salmon.toSet)
    assert(sparkSession.avro[Salmon](multi).collect().toSet == salmon.toSet)
  }

  test("jackson read/write identity") {
    val single = "./data/test/spark/cap/jackson/jackson.json"
    val multi  = "./data/test/spark/cap/jackson/multi.jackson"
    val rdd    = sparkSession.sparkContext.parallelize(salmon)
    val prepare = fileSink[IO](blocker).delete(single) >>
      rdd.single[IO](blocker).jackson(single) >>
      fileSink[IO](blocker).delete(multi) >>
      rdd.multi[IO](blocker).jackson(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.jackson[Salmon](single).collect().toSet == salmon.toSet)
    assert(sparkSession.jackson[Salmon](multi).collect().toSet == salmon.toSet)
  }

  test("unable to save to parquet (happy failure)") {
    val single = "./data/test/spark/cap/parquet/single.parquet"
    val rdd    = sparkSession.sparkContext.parallelize(salmon)
    val prepare = fileSink[IO](blocker).delete(single) >>
      rdd.single[IO](blocker).parquet(single)
    assertThrows[Exception](prepare.unsafeRunSync())
  }

  test("circe read/write unequal (happy failure)") {
    val single = "./data/test/spark/cap/circe/circe.json"
    val multi  = "./data/test/spark/cap/circe/multi.circe"

    val rdd = sparkSession.sparkContext.parallelize(salmon)
    val prepare = fileSink[IO](blocker).delete(single) >>
      rdd.single[IO](blocker).circe(single) >>
      fileSink[IO](blocker).delete(multi) >>
      rdd.multi[IO](blocker).circe(multi)
    prepare.unsafeRunSync()
    assert(sparkSession.circe[Salmon](single).collect().toSet != salmon.toSet)
    assert(sparkSession.circe[Salmon](multi).collect().toSet != salmon.toSet)
  }
}
