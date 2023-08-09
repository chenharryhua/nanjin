package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopJackson, JacksonFile, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
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
