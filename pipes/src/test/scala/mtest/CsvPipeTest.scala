package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{CsvDeserialization, CsvSerialization}
import fs2.Stream
import kantan.csv._
import org.scalatest.funsuite.AnyFunSuite
import kantan.csv.generic._

class CsvPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new CsvSerialization[IO, Tigger](RowEncoder[Tigger],CsvConfiguration.rfc, blocker)
  val dser = new CsvDeserialization[IO, Tigger](RowDecoder[Tigger],CsvConfiguration.rfc)

  test("csv identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)
    assert(
      data
        .through(ser.serialize)
        .through(dser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
