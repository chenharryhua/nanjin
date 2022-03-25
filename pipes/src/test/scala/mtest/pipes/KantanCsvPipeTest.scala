package mtest.pipes

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.KantanSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*
import kantan.csv.generic.*

class KantanCsvPipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  val hd                      = NJHadoop[IO](new Configuration())

  test("csv identity") {

    assert(
      data
        .through(KantanSerde.toBytes[IO, Tiger](CsvConfiguration.rfc, 300.kb))
        .through(KantanSerde.fromBytes[IO, Tiger](CsvConfiguration.rfc, 5))
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("csv identity akka") {
    import mtest.terminals.mat
    assert(
      IO.fromFuture(
        IO(
          Source(tigers)
            .via(KantanSerde.akka.toByteString(CsvConfiguration.rfc))
            .via(KantanSerde.akka.fromByteString[Tiger](CsvConfiguration.rfc))
            .runFold(List.empty[Tiger]) { case (ss, i) => ss.appended(i) }))
        .unsafeRunSync() === tigers)
  }

  test("csv identity akka header") {
    import mtest.terminals.mat
    val rfc = CsvConfiguration.rfc.withHeader("a", "b", "c")
    assert(
      IO.fromFuture(
        IO(
          Source(tigers)
            .via(KantanSerde.akka.toByteString(rfc))
            .via(KantanSerde.akka.fromByteString[Tiger](rfc))
            .runFold(List.empty[Tiger]) { case (ss, i) => ss.appended(i) }))
        .unsafeRunSync() === tigers)
  }

  val root = NJPath("./data/pipes/csv/")
  test("write/read identity csv") {
    val path = root / "csv.csv"
    hd.delete(path).unsafeRunSync()
    val tigers = List(Tiger(1, Some("a|b")), Tiger(2, Some("a'b")), Tiger(3, None), Tiger(4, Some("a||'b")))
    val data   = Stream.emits(tigers).covaryAll[IO, Tiger]
    val rfc = CsvConfiguration.rfc
      .withHeader("a", "b", "c")
      .withCellSeparator('|')
      .withQuote('\'')
      .withQuotePolicy(CsvConfiguration.QuotePolicy.Always)
    val write =
      data.through(KantanSerde.toBytes[IO, Tiger](rfc, 2.kb)).through(hd.bytes.sink(path))
    val read = hd.bytes.source(path).through(KantanSerde.fromBytes[IO, Tiger](rfc, 1))
    val run  = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity csv header") {
    val path = root / "header.csv"
    hd.delete(path).unsafeRunSync()
    val tigers = List(Tiger(1, Some("a|b")), Tiger(2, Some("a'b")), Tiger(3, None), Tiger(4, Some("a||'b")))
    val data   = Stream.emits(tigers).covaryAll[IO, Tiger]
    val rfc    = CsvConfiguration.rfc.withoutHeader
    val write  = data.through(KantanSerde.toBytes[IO, Tiger](rfc, 2.kb)).through(hd.bytes.sink(path))
    val read   = hd.bytes.source(path).through(KantanSerde.fromBytes[IO, Tiger](rfc, 1))
    val run    = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}