package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{NJCirce, NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import mtest.pipes.TestData
import mtest.pipes.TestData.Tiger
import mtest.terminals.HadoopTestData.hdp
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt
class NJCirceTest extends AnyFunSuite {

  val json: NJCirce[IO, Tiger] = hdp.circe[Tiger](isKeepNull = true)

  def fs2(path: NJPath, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO]
    val sink   = json.withCompressionLevel(3).sink(path)
    val src    = json.source(path)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/circe/tiger")

  val fmt = NJFileFormat.Circe

  test("uncompressed") {
    fs2(fs2Root / Uncompressed.fileName(fmt), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root / Gzip.fileName(fmt), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root / Snappy.fileName(fmt), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root / Bzip2.fileName(fmt), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root / Lz4.fileName(fmt), TestData.tigerSet)
  }

  test("deflate") {
    fs2(fs2Root / Deflate(1).fileName(fmt), TestData.tigerSet)
  }

  ignore("zstandard") {
    fs2(fs2Root / Zstandard(1).fileName(fmt), TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.json")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).circe[Tiger](isKeepNull = true)
    Stream.emits(TestData.tigerSet.toList).covary[IO].through(conn.sink(path)).compile.drain.unsafeRunSync()
  }

  test("laziness") {
    json.source(NJPath("./does/not/exist"))
    json.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(json.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / s"${t.index}.${Uncompressed.fileName(fmt)}"))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(json.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 10)
  }
}
