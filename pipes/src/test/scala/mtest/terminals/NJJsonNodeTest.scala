package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.terminals.{FileKind, JacksonFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class NJJsonNodeTest extends AnyFunSuite {
  import HadoopTestData.*

  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: JacksonFile, data: Set[JsonNode]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink = hdp.sink(tgt).jsonNode
    val src = hdp.source(tgt).jsonNode(10)
    val ts = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()

    hdp.source(tgt).jsonNode(100).debug().compile.drain.unsafeRunSync()

    assert(size == data.size)
    assert(hdp.source(tgt).jackson(10, pandaSchema).compile.toList.unsafeRunSync().toSet == data)
  }
}
