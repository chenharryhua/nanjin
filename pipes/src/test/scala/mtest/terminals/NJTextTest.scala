package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopText, NJHadoop, NJPath, TextFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import mtest.pipes.TestData
import mtest.pipes.TestData.Tiger
import mtest.terminals.HadoopTestData.hdp
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt
class NJTextTest extends AnyFunSuite {

  val text: HadoopText[IO] = hdp.text

  def fs2(path: NJPath, file: TextFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces)
    val sink   = text.withCompressionLevel(file.compression.compressionLevel).sink(tgt)
    val src    = text.source(tgt).mapFilter(decode[Tiger](_).toOption)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/text/tiger")

  test("uncompressed") {
    fs2(fs2Root, TextFile(Uncompressed), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root, TextFile(Gzip), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root, TextFile(Snappy), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root, TextFile(Bzip2), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root, TextFile(Lz4), TestData.tigerSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, TextFile(Deflate(1)), TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.json")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).circe
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .map(_.asJson)
      .through(conn.sink(path))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("laziness") {
    text.source(NJPath("./does/not/exist"))
    text.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(Uncompressed)
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson.noSpaces)
      .through(text.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / fk.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(text.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 10)
  }
}
