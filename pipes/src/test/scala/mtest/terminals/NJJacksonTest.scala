package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopJackson, JacksonFile, NJFileKind, NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJJacksonTest extends AnyFunSuite {
  import HadoopTestData.*

  val jackson: HadoopJackson[IO] = hdp.jackson(pandaSchema)

  def fs2(path: NJPath, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink =
      jackson.withCompressionLevel(file.compression.compressionLevel).withBlockSizeHint(1000).sink(tgt)
    val src    = jackson.source(tgt)
    val ts     = Stream.emits(data.toList).covary[IO].chunks
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/jackson/panda")
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
      .through(jackson.sink(policies.fixedDelay(1.second), ZoneId.systemDefault())(t =>
        path / fk.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size =
      Stream.eval(hdp.filesIn(path)).flatMap(jackson.source).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
