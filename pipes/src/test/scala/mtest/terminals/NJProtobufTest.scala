package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{FileKind, ProtobufFile}
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.proto.test_message.TestAnimal
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*

class NJProtobufTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  val animals: List[TestAnimal] = List(
    TestAnimal(name = "panda", age = 5, zoo = "ChengDu"),
    TestAnimal(name = "tiger", age = 8, zoo = "Beijing")
  )

  val animalSet: Set[TestAnimal] = animals.toSet

  def fs2(path: Url, file: ProtobufFile, data: Set[TestAnimal]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).protobuf
    val src = hdp.source(tgt).protobuf[TestAnimal](100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(hdp.source(tgt).protobuf[TestAnimal](100).compile.toList.unsafeRunSync().toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/protobuf/animal")

  test("1.uncompressed") {
    fs2(fs2Root, ProtobufFile(_.Uncompressed), animalSet)
  }

  test("2.gzip") {
    fs2(fs2Root, ProtobufFile(_.Gzip), animalSet)
  }

  test("3.snappy") {
    fs2(fs2Root, ProtobufFile(_.Snappy), animalSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, ProtobufFile(_.Bzip2), animalSet)
  }

  test("5.lz4") {
    fs2(fs2Root, ProtobufFile(_.Lz4), animalSet)
  }

  test("6.deflate") {
    fs2(fs2Root, ProtobufFile(_.Deflate(_.Seven)), animalSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").protobuf[TestAnimal](100)
    hdp.sink("./does/not/exist").protobuf
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = ProtobufFile(_.Uncompressed)
    val processedSize = Stream
      .emits(animals)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(zoneId, _.fixedDelay(0.1.second))(t => path / file.fileName(t)).protobuf)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).protobuf[TestAnimal](100).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = ProtobufFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val processedSize = Stream
      .emits(animals)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).protobuf)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).protobuf[TestAnimal](100).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("10.stream concat") {
    val s = Stream.emits(animals).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "data.pb"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).protobuf).compile.drain).unsafeRunSync()
    val size =
      hdp.source(path).protobuf[TestAnimal](100).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 3000)
  }
}
