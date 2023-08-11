package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{KantanFile, NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJKantanTest extends AnyFunSuite {
  implicit val tigerEncoder: RowEncoder[Tiger] = shapeless.cachedImplicit
  implicit val tigerDecoder: RowDecoder[Tiger] = shapeless.cachedImplicit

  def fs2(path: NJPath, file: KantanFile, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts   = Stream.emits(data.toList).covary[IO].map(tigerEncoder.encode).chunks
    val sink = hdp.kantan(csvConfiguration).withCompressionLevel(file.compression.compressionLevel).sink(tgt)
    val src  = hdp.kantan(csvConfiguration).source(tgt, 100).map(tigerDecoder.decode).rethrow
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/csv/tiger")

  test("uncompressed") {
    val cfg = CsvConfiguration.rfc.withHeader("a", "b", "c")
    fs2(fs2Root, KantanFile(Uncompressed), cfg, TestData.tigerSet)
  }

  test("gzip") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root, KantanFile(Gzip), cfg, TestData.tigerSet)
  }
  test("snappy") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root, KantanFile(Snappy), cfg, TestData.tigerSet)
  }
  test("bzip2") {
    val cfg = CsvConfiguration.rfc.withCellSeparator('?')
    fs2(fs2Root, KantanFile(Bzip2), cfg, TestData.tigerSet)
  }
  test("lz4") {
    val cfg = CsvConfiguration.rfc.withQuotePolicy(CsvConfiguration.QuotePolicy.WhenNeeded)
    fs2(fs2Root, KantanFile(Lz4), cfg, TestData.tigerSet)
  }

  test("deflate") {
    val cfg = CsvConfiguration.rfc.withQuote('*')
    fs2(fs2Root, KantanFile(Deflate(1)), cfg, TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.csv")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).kantan(CsvConfiguration.rfc)
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .map(tigerEncoder.encode)
      .chunks
      .through(conn.sink(path))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("laziness") {
    hdp.kantan(CsvConfiguration.rfc).source(NJPath("./does/not/exist"), 100)
    hdp.kantan(CsvConfiguration.rfc).sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val csv    = hdp.kantan(CsvConfiguration.rfc.withHeader(true))
    val path   = fs2Root / "rotation"
    val number = 10000L
    val file   = KantanFile(Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(tigerEncoder.encode)
      .chunks
      .through(csv.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / file.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size =
      Stream.force(hdp.filesIn(path).map(csv.source(_, 100))).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 10)
  }
}
