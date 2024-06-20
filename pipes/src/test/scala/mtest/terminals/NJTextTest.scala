package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.jawn
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
class NJTextTest extends AnyFunSuite {

  val text: HadoopText[IO] = hdp.text

  def fs2(path: NJPath, file: TextFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts                      = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces).chunks
    val sink                    = text.sink(tgt)
    val src: Stream[IO, Tiger]  = text.source(tgt, 2).mapFilter(decode[Tiger](_).toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/text/tiger")

  test("uncompressed") {
    fs2(fs2Root, TextFile(_.Uncompressed), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root, TextFile(_.Gzip), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root, TextFile(_.Snappy), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root, TextFile(_.Bzip2), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root, TextFile(_.Lz4), TestData.tigerSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, TextFile(_.Deflate(8)), TestData.tigerSet)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data/tiger.txt")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    val conn = NJHadoop[IO](conf).text
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .map(_.toString)
      .chunks
      .through(conn.sink(path))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("laziness") {
    text.source(NJPath("./does/not/exist"), 1)
    text.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation" / "data"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(Uncompressed)
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.toString)
      .chunks
      .through(text.sink(policies.fixedDelay(1.second), ZoneId.systemDefault())(t => path / fk.fileName(t)))
      .fold(0)(_ + _)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(text.source(_, 2).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 10)
    assert(processedSize == number * 10)

  }

  test("rotation - empty") {
    val text = hdp.text
    val path = fs2Root / "rotation" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, String]).chunks
      .through(text.sink(policies.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
        path / fk.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.uri).lines.isEmpty))
  }
}
