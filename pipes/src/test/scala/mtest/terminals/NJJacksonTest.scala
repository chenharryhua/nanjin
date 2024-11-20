package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopJackson, JacksonFile, NJFileKind, NJHadoop}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJJacksonTest extends AnyFunSuite {
  import HadoopTestData.*

  val jackson: HadoopJackson[IO] = hdp.jackson(pandaSchema)

  def fs2(path: Url, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink =
      jackson.sink(tgt)
    val src    = jackson.source(tgt, 10)
    val ts     = Stream.emits(data.toList).covary[IO].chunks
    val action = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)

  }

  val fs2Root: Url = Url.parse("./data/test/terminals/jackson/panda")
  test("uncompressed") {
    fs2(fs2Root, JacksonFile(_.Uncompressed), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root, JacksonFile(_.Gzip), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root, JacksonFile(_.Snappy), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root, JacksonFile(_.Bzip2), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root, JacksonFile(_.Lz4), pandaSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, JacksonFile(_.Deflate(5)), pandaSet)
  }

  test("ftp - parse username/password") {
    val path = Url.parse("ftp://chenh:test@localhost/data/tiger.jackson.json")
    val conf = new Configuration()
    // conf.set("fs.ftp.host", "localhost")
    // conf.set("fs.ftp.user.localhost", "chenh")
    // conf.set("fs.ftp.password.localhost", "test")
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
    jackson.source("./does/not/exist", 10)
    jackson.sink("./does/not/exist")
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = JacksonFile(Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(jackson.sink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / fk.fileName(t)))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(jackson.source(_, 10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }
}
