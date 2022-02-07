package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class CsvPipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  test("csv identity") {

    assert(
      data
        .through(CsvSerde.serPipe[IO, Tiger](CsvConfiguration.rfc, 300.kb))
        .through(CsvSerde.deserPipe[IO, Tiger](CsvConfiguration.rfc, 5))
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("write/read identity csv") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/csv.csv")
    val write = data.through(CsvSerde.serPipe[IO, Tiger](CsvConfiguration.rfc, 2.kb)).through(hd.byteSink(path))
    val read  = hd.byteSource(path).through(CsvSerde.deserPipe[IO, Tiger](CsvConfiguration.rfc, 1))
    val run   = hd.delete(path) >> write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
