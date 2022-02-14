package mtest.terminals
import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import mtest.pipes.TestData
import mtest.pipes.TestData.Tiger
import mtest.terminals.HadoopTestData.hdp
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class NJCsvTest extends AnyFunSuite {
  def akka(path: NJPath, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts   = Source(data)
    val sink = hdp.csv(csvConfiguration).akka.sink[Tiger](path)
    val src  = hdp.csv(csvConfiguration).akka.source[Tiger](path)
    val action = IO.fromFuture(IO(ts.runWith(sink))) >>
      IO.fromFuture(IO(src.runFold(Set.empty[Tiger]) { case (ss, i) => ss + i }))
    assert(action.unsafeRunSync() == data)
  }

  def fs2(path: NJPath, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO]
    val sink   = hdp.csv(csvConfiguration).sink[Tiger](path)
    val src    = hdp.csv(csvConfiguration).source[Tiger](path)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val akkaRoot: NJPath = NJPath("./data/test/pipes/csv/akka")
  val fs2Root: NJPath  = NJPath("./data/test/pipes/csv/fs2")

  test("uncompressed") {
    val cfg = CsvConfiguration.rfc
    akka(akkaRoot / "tiger.csv", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv", cfg, TestData.tigerSet)
  }

  test("gzip") {
    val cfg = CsvConfiguration.rfc
    akka(akkaRoot / "tiger.csv.gz", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv.gz", cfg, TestData.tigerSet)
  }
  test("snappy") {
    val cfg = CsvConfiguration.rfc.withHeader("a", "b", "c")
    akka(akkaRoot / "tiger.csv.snappy", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv.snappy", cfg, TestData.tigerSet)
  }
  test("bzip2") {
    val cfg = CsvConfiguration.rfc.withCellSeparator('?')
    akka(akkaRoot / "tiger.csv.bz2", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv.bz2", cfg, TestData.tigerSet)
  }
  test("lz4") {
    val cfg = CsvConfiguration.rfc.withQuotePolicy(CsvConfiguration.QuotePolicy.WhenNeeded)
    akka(akkaRoot / "tiger.csv.lz4", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv.lz4", cfg, TestData.tigerSet)
  }

  test("deflate") {
    val cfg = CsvConfiguration.rfc.withQuote('*')
    fs2(fs2Root / "tiger.csv.deflate", cfg, TestData.tigerSet)
    akka(akkaRoot / "tiger.csv.deflate", cfg, TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    val cfg = CsvConfiguration.rfc
    akka(akkaRoot / "tiger.csv.zst", cfg, TestData.tigerSet)
    fs2(fs2Root / "tiger.csv.zst", cfg, TestData.tigerSet)
  }
}
