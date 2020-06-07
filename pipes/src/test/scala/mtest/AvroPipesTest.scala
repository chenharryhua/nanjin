package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.AvroSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

object AvroPipesTestData {
  case class Test(a: Int, b: String)

  val list = (1 to 10).map(x => Test(x, List.fill(x)("a").mkString("\n")))

  val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)
}

class AvroPipesTest extends AnyFunSuite {
  import AvroPipesTestData._
  val pipe = new AvroSerialization[IO, Test](blocker)

  data.through(pipe.toPrettyJson).showLinesStdOut.compile.drain.unsafeRunSync()
  test("jackson identity") {
    data.through(pipe.toByteJson).through(pipe.fromJson).compile.toList === list
  }

  test("data identity") {
    data
      .through(pipe.toData)
      .chunkLimit(3)
      .map(_.toArray)
      .through(pipe.fromData)
      .compile
      .toList === list
  }
  test("binary identity") {
    data
      .through(pipe.toBinary)
      .chunkLimit(2)
      .map(_.toArray)
      .through(pipe.fromBinary)
      .compile
      .toList === list
  }
}
