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
  val ser                      = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)
  val dser                     = new CsvDeserialization[IO, Tigger](CsvConfiguration.rfc)
  val data: Stream[IO, Tigger] = Stream.fromIterator[IO](list.iterator)

  test("csv identity") {
    assert(
      data.through(ser.serialize).through(dser.deserialize).compile.toList.unsafeRunSync() === list)
  }
}
