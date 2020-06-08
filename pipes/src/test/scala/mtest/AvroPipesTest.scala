package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{AvroDeserialization, AvroSerialization}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

object TestData {
  case class Test(a: Int, b: String)

  val list: List[Test] = (1 to 10).map(x => Test(x, List.fill(x)("a").mkString("\n"))).toList

  val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)
}

class AvroPipesTest extends AnyFunSuite {
  import TestData._
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
