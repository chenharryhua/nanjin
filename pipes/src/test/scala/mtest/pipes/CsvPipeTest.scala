package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.CsvSerialization
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import kantan.csv.CsvConfiguration

class CsvPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val ser = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)

  "csv pipe" - {
    "identity" in {
      val data: Stream[IO, Tigger] = Stream.emits(tiggers)
 
      val run = data.through(ser.serialize).through(ser.deserialize).compile.toList
      run.asserting(_ shouldBe tiggers)
    }
  }
}
