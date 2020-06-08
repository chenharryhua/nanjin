package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{AvroDeserialization, AvroSerialization}
import fs2.{Chunk, Stream}
import org.scalatest.funsuite.AnyFunSuite

object AvroPipesTestData {
  case class Test(a: Int, b: String)

  val list = (1 to 10).map(x => Test(x, List.fill(x)("a").mkString("\n")))

  val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)
}

class AvroPipesTest extends AnyFunSuite {
  import AvroPipesTestData._
  val ser  = new AvroSerialization[IO, Test]
  val dser = new AvroDeserialization[IO, Test]

  data.through(ser.toPrettyJson).showLinesStdOut.compile.drain.unsafeRunSync()
  test("jackson identity") {
    data.through(ser.toByteJson).through(dser.fromJackson).compile.toList === list
  }

  test("binary identity") {
    data.through(ser.toBinary).through(dser.fromBinary).compile.toList === list
  }
}
