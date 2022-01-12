package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerialization
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class CsvPipeTest extends AnyFunSuite {
  import TestData.*
  val ser = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)

  test("csv identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)
    assert(data.through(ser.serialize(300.kb)).through(ser.deserialize(5)).compile.toList.unsafeRunSync() === tiggers)
  }
}
