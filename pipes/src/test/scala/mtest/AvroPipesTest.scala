package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{AvroDeserialization, AvroSerialization}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object TestData {
  case class Test(a: Int, b: String)

  val list: List[Test] =
    (1 to 10).map(x => Test(Random.nextInt(), "a")).toList

}

class AvroPipesTest extends AnyFunSuite {
  import TestData._
  val ser  = new AvroSerialization[IO, Test]
  val dser = new AvroDeserialization[IO, Test]

  test("jackson identity") {
    val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)

    assert(
      data
        .through(ser.toByteJson)
        .through(dser.fromJackson)
        .compile
        .toList
        .unsafeRunSync() === list)
  }

  test("binary identity") {
    val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)

    assert(
      data.through(ser.toBinary).through(dser.fromBinary).compile.toList.unsafeRunSync() === list)
  }
}
