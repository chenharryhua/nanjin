package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerialization
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.scalatest.funsuite.AnyFunSuite

class CsvPipeTest extends AnyFunSuite {
  import TestData.*
  val ser = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc, 300)

  test("csv identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)
    assert(data.through(ser.serialize).through(ser.deserialize).compile.toList.unsafeRunSync() === tiggers)
  }
}
