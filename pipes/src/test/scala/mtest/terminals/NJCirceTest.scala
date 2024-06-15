package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.text.{lines, utf8}
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.circe.{jawn, Json}
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
class NJCirceTest extends AnyFunSuite {

  val json: HadoopCirce[IO] = hdp.circe

  def fs2(path: NJPath, file: CirceFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts                      = Stream.emits(data.toList).covary[IO].map(_.asJson)
    val sink                    = json.sink(tgt)
    val src: Stream[IO, Tiger]  = json.source(tgt, 2.bytes).mapFilter(_.as[Tiger].toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val lines = hdp.text.source(tgt, 32).compile.fold(0) { case (s, _) => s + 1 }
    assert(lines.unsafeRunSync() === data.size)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/circe/tiger")

  test("uncompressed") {
    fs2(fs2Root, CirceFile(_.Uncompressed), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root, CirceFile(_.Gzip), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root, CirceFile(_.Snappy), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root, CirceFile(_.Bzip2), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root, CirceFile(_.Lz4), TestData.tigerSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, CirceFile(_.Deflate(4)), TestData.tigerSet)
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
    json.source(NJPath("./does/not/exist"), 100.bytes)
    json.sink(NJPath("./does/not/exist"))
  }

  test("rotation - data") {
    val path   = fs2Root / "rotation" / "data"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = CirceFile(Uncompressed)
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson)
      .through(json.sink(policies.fixedDelay(1.second), ZoneId.systemDefault())(t => path / fk.fileName(t)))
      .fold(0)(_ + _)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(json.source(_, 5.megabytes).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * TestData.tigerSet.toList.size)
    assert(processedSize == number * TestData.tigerSet.toList.size)

    def countTiger(path: NJPath): IO[Int] = hdp.bytes
      .source(path, 64)
      .through(utf8.decode)
      .through(lines)
      .map(jawn.decode[Tiger])
      .rethrow
      .map(_ => 1)
      .compile
      .fold(0)(_ + _)

    val num = hdp.filesIn(path).flatMap(_.traverse(countTiger)).unsafeRunSync().sum

    assert(num == number * TestData.tigerSet.toList.size)
  }

  test("rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = CirceFile(Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, Json])
      .through(json.sink(policies.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
        path / fk.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.uri).lines.isEmpty))
  }
}
