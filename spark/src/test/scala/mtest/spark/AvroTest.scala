package mtest.spark

import java.time.{Instant, LocalDate}
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.datetime._

import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil}

object AvroTestData {
  case object Atlantic
  case object Chinook
  case object Chum

  type Loc = Atlantic.type :+: Chinook.type :+: Chum.type :+: CNil

  final case class Salmon(from: Loc, birth: LocalDate, now: Instant)
}

class AvroTest extends AnyFunSuite {
  test("single file test") {}
}
