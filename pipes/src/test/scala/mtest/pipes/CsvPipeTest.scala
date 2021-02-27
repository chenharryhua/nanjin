package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.CsvSerialization
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.scalatest.funsuite.AnyFunSuite

class CsvPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)

  test("identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    val run = data.through(ser.serialize).through(ser.deserialize).compile.toList
    assert(run.unsafeRunSync() == tiggers)
  }
}
