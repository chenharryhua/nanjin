package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{FileKind, JacksonFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationDouble

class NJJsonNodeTest extends AnyFunSuite {
  import HadoopTestData.*

  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink = hdp.sink(tgt).jsonNode
    val src = hdp.source(tgt).jsonNode(10)
    val ts = Stream.emits(data.toList.flatMap(genericRecord2JsonNode(_).toOption)).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
      .map(_.flatMap(jsonNode2GenericRecord(_, pandaSchema).toOption))
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()

    hdp.source(tgt).jsonNode(100).debug().compile.drain.unsafeRunSync()

    assert(size == data.size)
    assert(
      hdp
        .source(tgt)
        .jsonNode(10)
        .compile
        .toList
        .unsafeRunSync()
        .flatMap(jsonNode2GenericRecord(_, pandaSchema).toOption)
        .toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/json-node/panda")
  test("1.uncompressed") {
    fs2(fs2Root, JacksonFile(_.Uncompressed), pandaSet)
  }

  test("2.gzip") {
    fs2(fs2Root, JacksonFile(_.Gzip), pandaSet)
  }

  test("3.snappy") {
    fs2(fs2Root, JacksonFile(_.Snappy), pandaSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, JacksonFile(_.Bzip2), pandaSet)
  }

  test("5.lz4") {
    fs2(fs2Root, JacksonFile(_.Lz4), pandaSet)
  }

  test("6.deflate - 1") {
    fs2(fs2Root, JacksonFile(_.Deflate(5)), pandaSet)
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = JacksonFile(_.Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList.flatMap(genericRecord2JsonNode(_).toOption))
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(zoneId, _.fixedDelay(0.2.second))(t => path / file.fileName(t)).jsonNode)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).jsonNode(10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = JacksonFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val tickedValues = Stream
      .emits(pandaSet.toList.flatMap(genericRecord2JsonNode(_).toOption))
      .covary[IO]
      .repeatN(number)
      .chunkN(300)
      .unchunks
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).jsonNode)
      .compile
      .toList
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).jackson(10, pandaSchema).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(tickedValues.map(_.value.recordCount).sum == number * 2)

    assert(tickedValues.head.value.recordCount == 1000)
    assert(tickedValues.head.value.url.path.parts.toList.last.contains("0001"))
    assert(tickedValues.head.tick.index == 1)
    assert(tickedValues(1).value.recordCount == 1000)
    assert(tickedValues(1).value.url.path.parts.toList.last.contains("0002"))
    assert(tickedValues(1).tick.index == 2)
    assert(tickedValues(2).value.recordCount == 1000)
    assert(tickedValues(2).value.url.path.parts.toList.last.contains("0003"))
    assert(tickedValues(2).tick.index == 3)
    assert(tickedValues(3).value.recordCount == 1000)
    assert(tickedValues(3).tick.index == 4)

    assert(tickedValues(4).value.recordCount == 1000)
    assert(tickedValues(4).tick.index == 5)
    assert(tickedValues(5).value.recordCount == 1000)
    assert(tickedValues(5).tick.index == 6)
    assert(tickedValues(6).value.recordCount == 1000)
    assert(tickedValues(6).tick.index == 7)
    assert(tickedValues(7).value.recordCount == 1000)
    assert(tickedValues(7).tick.index == 8)

    assert(tickedValues(8).value.recordCount == 1000)
    assert(tickedValues(8).tick.index == 9)
    assert(tickedValues(9).value.recordCount == 1000)
    assert(tickedValues(9).tick.index == 10)

    assert(tickedValues.last.value.recordCount == 0)

  }
}
