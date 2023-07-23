package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJHeaderEncoder, NJPath}
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
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJCsvTest extends AnyFunSuite {
  implicit val tigerEncoder: NJHeaderEncoder[Tiger] = shapeless.cachedImplicit

  def fs2(path: NJPath, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO]
    val sink   = hdp.kantan[Tiger](csvConfiguration).withChunkSize(100).withCompressionLevel(3).sink(path)
    val src    = hdp.kantan[Tiger](csvConfiguration).source(path)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/csv/tiger")
  val fmt             = NJFileFormat.Kantan

  test("uncompressed") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / Uncompressed.fileName(fmt), cfg, TestData.tigerSet)
  }

  test("gzip") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / Gzip.fileName(fmt), cfg, TestData.tigerSet)
  }
  test("snappy") {
    val cfg = CsvConfiguration.rfc.withHeader("a", "b", "c")
    fs2(fs2Root / Snappy.fileName(fmt), cfg, TestData.tigerSet)
  }
  test("bzip2") {
    val cfg = CsvConfiguration.rfc.withCellSeparator('?')
    fs2(fs2Root / Bzip2.fileName(fmt), cfg, TestData.tigerSet)
  }
  test("lz4") {
    val cfg = CsvConfiguration.rfc.withQuotePolicy(CsvConfiguration.QuotePolicy.WhenNeeded)
    fs2(fs2Root / Lz4.fileName(fmt), cfg, TestData.tigerSet)
  }

  test("deflate") {
    val cfg = CsvConfiguration.rfc.withQuote('*')
    fs2(fs2Root / Deflate(1).fileName(fmt), cfg, TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root / Zstandard(1).fileName(fmt), cfg, TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.csv")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).kantan[Tiger](CsvConfiguration.rfc)
    Stream.emits(TestData.tigerSet.toList).covary[IO].through(conn.sink(path)).compile.drain.unsafeRunSync()
  }

  test("laziness") {
    hdp.kantan[Tiger](CsvConfiguration.rfc).source(NJPath("./does/not/exist"))
    hdp.kantan[Tiger](CsvConfiguration.rfc).sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val csv    = hdp.kantan[Tiger](CsvConfiguration.rfc.withHeader)
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(csv.sink(RetryPolicies.constantDelay[IO](1.second))(t =>
        path / s"${t.index}.${Uncompressed.fileName(fmt)}"))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(csv.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 10)
  }
}
