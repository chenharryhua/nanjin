package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{BinAvroFile, NJFileKind, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJBinAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  private val binAvro = hdp.binAvro(pandaSchema)

  def fs2(path: NJPath, file: BinAvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName

    hdp.delete(tgt).unsafeRunSync()
    val sink =
      binAvro.sink(tgt)
    val src      = binAvro.source(tgt, 100)
    val ts       = Stream.emits(data.toList).covary[IO].chunks
    val action   = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    assert(action.unsafeRunSync().toSet == data)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/bin_avro/panda")

  test("uncompressed") {
    fs2(fs2Root, BinAvroFile(_.Uncompressed), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root, BinAvroFile(_.Gzip), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root, BinAvroFile(_.Snappy), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root, BinAvroFile(_.Bzip2), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root, BinAvroFile(_.Lz4), pandaSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, BinAvroFile(_.Deflate(2)), pandaSet)
  }

  test("laziness") {
    binAvro.source(NJPath("./does/not/exist"), 10)
    binAvro.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = BinAvroFile(Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(binAvro.sink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t =>
        path / file.fileName(t)))
      .fold(0)(_ + _)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(binAvro.source(_, 10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }
}
