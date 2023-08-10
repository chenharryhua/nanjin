package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopJackson, JacksonFile, NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import mtest.pipes.TestData
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJJacksonTest extends AnyFunSuite {
  import HadoopTestData.*

  val jackson: HadoopJackson[IO] = hdp.jackson(pandaSchema)

  def fs2(path: NJPath, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink   = jackson.withCompressionLevel(file.compression.compressionLevel).sink(tgt)
    val src    = jackson.source(tgt)
    val ts     = Stream.emits(data.toList).covary[IO].chunks
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/jackson/panda")
  test("uncompressed") {
    fs2(fs2Root, JacksonFile(Uncompressed), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root, JacksonFile(Gzip), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root, JacksonFile(Snappy), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root, JacksonFile(Bzip2), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root, JacksonFile(Lz4), pandaSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, JacksonFile(Deflate(1)), pandaSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.jackson.json")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).jackson(TestData.Tiger.avroEncoder.schema)
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .map(TestData.Tiger.to.to)
      .chunks
      .through(conn.sink(path))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("laziness") {
    jackson.source(NJPath("./does/not/exist"))
    jackson.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = JacksonFile(Uncompressed)
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(jackson.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / fk.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(jackson.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
