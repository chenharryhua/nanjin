package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{FileKind, JacksonFile}
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*

class NJJsonNodeTest extends AnyFunSuite {
  import HadoopTestData.*

  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink = hdp.sink(tgt).jsonNode(objectMapper.writer())
    val src = hdp.source(tgt).jsonNode(10, objectMapper.reader())
    val ts = Stream.emits(data.toList.flatMap(genericRecord2JsonNode(_).toOption)).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
      .map(_.flatMap(jsonNode2GenericRecord(_, pandaSchema).toOption))
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()

    hdp.source(tgt).jsonNode(100, objectMapper.reader()).debug().compile.drain.unsafeRunSync()

    assert(size == data.size)
    assert(
      hdp
        .source(tgt)
        .jsonNode(10, objectMapper.reader())
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
    fs2(fs2Root, JacksonFile(_.Deflate(_.Nine)), pandaSet)
  }

  test("7.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = JacksonFile(_.Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList.flatMap(genericRecord2JsonNode(_).toOption))
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(zoneId, _.fixedDelay(0.2.second))(t => path / file.fileName(t))
        .jsonNode(objectMapper.writer()))
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).jsonNode(10, objectMapper.reader()).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("8.jsonNode source skips embedded empty lines") {
    val path = fs2Root / "empty-lines"
    val tgt = path / "with-blanks.jackson.json"
    hdp.delete(tgt).unsafeRunSync()

    // Write a file with embedded blank lines between valid JSON objects
    val records = pandaSet.toList.flatMap(genericRecord2JsonNode(_).toOption)
    val linesWithBlanks: List[String] =
      records.map(objectMapper.writer().writeValueAsString(_)).flatMap(line => List(line, "", ""))

    Stream
      .emits(linesWithBlanks)
      .covary[IO]
      .through(hdp.sink(tgt).text)
      .compile
      .drain
      .unsafeRunSync()

    // Read back — should skip blank lines and return all records
    val result = hdp.source(tgt).jsonNode(10, objectMapper.reader()).compile.toList.unsafeRunSync()
    val roundTripped = result.flatMap(jsonNode2GenericRecord(_, pandaSchema).toOption).toSet
    assert(roundTripped == pandaSet)
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
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t))
        .jsonNode(objectMapper.writer()))
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
    assert(tickedValues.map(_.recordCount).sum == number * 2)

    assert(tickedValues.head.recordCount == 1000)
    assert(tickedValues.head.url.path.parts.toList.last.contains("0001"))
    assert(tickedValues.head.create.index == 1)
    assert(tickedValues(1).recordCount == 1000)
    assert(tickedValues(1).url.path.parts.toList.last.contains("0002"))
    assert(tickedValues(1).create.index == 2)
    assert(tickedValues(2).recordCount == 1000)
    assert(tickedValues(2).url.path.parts.toList.last.contains("0003"))
    assert(tickedValues(2).create.index == 3)
    assert(tickedValues(3).recordCount == 1000)
    assert(tickedValues(3).create.index == 4)

    assert(tickedValues(4).recordCount == 1000)
    assert(tickedValues(4).create.index == 5)
    assert(tickedValues(5).recordCount == 1000)
    assert(tickedValues(5).create.index == 6)
    assert(tickedValues(6).recordCount == 1000)
    assert(tickedValues(6).create.index == 7)
    assert(tickedValues(7).recordCount == 1000)
    assert(tickedValues(7).create.index == 8)

    assert(tickedValues(8).recordCount == 1000)
    assert(tickedValues(8).create.index == 9)
    assert(tickedValues(9).recordCount == 1000)
    assert(tickedValues(9).create.index == 10)

    assert(tickedValues.last.recordCount == 1000)

  }
}
