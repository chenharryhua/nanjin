package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.CsvSerialization
import fs2.Stream
import kantan.csv._
import org.scalatest.funsuite.AnyFunSuite

class CsvPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)

  test("csv identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)
    assert(
      data
        .through(ser.serialize(blocker))
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
