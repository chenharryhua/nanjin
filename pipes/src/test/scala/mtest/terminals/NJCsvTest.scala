package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import mtest.pipes.TestData
import mtest.pipes.TestData.Tiger
import mtest.terminals.HadoopTestData.hdp
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class NJCsvTest extends AnyFunSuite {

  def fs2(path: NJPath, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO]
    val sink   = hdp.kantan(csvConfiguration).withChunkSize(100).withCompressionLevel(3).sink[Tiger](path)
    val src    = hdp.kantan(csvConfiguration).source[Tiger](path)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val akkaRoot: NJPath = NJPath("./data/test/pipes/csv/akka")
  val fs2Root: NJPath  = NJPath("./data/test/pipes/csv/fs2")

  test("uncompressed") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / "tiger.csv", cfg, TestData.tigerSet)
  }

  test("gzip") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / "tiger.csv.gz", cfg, TestData.tigerSet)
  }
  test("snappy") {
    val cfg = CsvConfiguration.rfc.withHeader("a", "b", "c")
    fs2(fs2Root / "tiger.csv.snappy", cfg, TestData.tigerSet)
  }
  test("bzip2") {
    val cfg = CsvConfiguration.rfc.withCellSeparator('?')
    fs2(fs2Root / "tiger.csv.bz2", cfg, TestData.tigerSet)
  }
  test("lz4") {
    val cfg = CsvConfiguration.rfc.withQuotePolicy(CsvConfiguration.QuotePolicy.WhenNeeded)
    fs2(fs2Root / "tiger.csv.lz4", cfg, TestData.tigerSet)
  }

  test("deflate") {
    val cfg = CsvConfiguration.rfc.withQuote('*')
    fs2(fs2Root / "tiger.csv.deflate", cfg, TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / "tiger.csv.zst", cfg, TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.csv")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost");
    conf.set("fs.ftp.user.localhost", "chenh");
    conf.set("fs.ftp.password.localhost", "test");
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE");
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem");
    val hdp = NJHadoop[IO](conf)
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .through(hdp.kantan(CsvConfiguration.rfc).sink(path))
      .compile
      .drain
      .unsafeRunSync()
  }
}
