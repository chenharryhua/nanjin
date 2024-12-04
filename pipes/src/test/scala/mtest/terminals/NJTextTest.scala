package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.jawn
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJTextTest extends AnyFunSuite {

  def fs2(path: Url, file: TextFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts                      = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces).chunks
    val sink                    = hdp.sink(tgt).text
    val src: Stream[IO, Tiger]  = hdp.source(tgt).text(2).mapFilter(decode[Tiger](_).toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(
      hdp
        .source(tgt)
        .text(100)
        .mapFilter(decode[Tiger](_).toOption)
        .compile
        .toList
        .unsafeRunSync()
        .toSet == data)
  }

  val fs2Root: Url = Url("./data/test/terminals/text/tiger")

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
    val path = Url.parse("ftp://localhost/data/tiger.txt")
    val conf = new Configuration()
    conf.set("fs.ftp.host", "localhost")
    conf.set("fs.ftp.user.localhost", "chenh")
    conf.set("fs.ftp.password.localhost", "test")
    conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem")
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .map(_.toString)
      .chunks
      .through(hdp.sink(path).text)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("laziness") {
    hdp.source("./does/not/exist").text(2)
    hdp.sink("./does/not/exist").text
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
      .through(
        hdp.rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / fk.fileName(t)).text)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).text(2).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 10)
    assert(processedSize == number * 10)

  }

  test("rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, String]).chunks
      .through(
        hdp
          .rotateSink(Policy.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
            path / fk.fileName(t))
          .text)
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }
}
