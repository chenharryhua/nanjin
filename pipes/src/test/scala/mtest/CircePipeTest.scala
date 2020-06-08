package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{CirceDeserialization, CirceSerialization}
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._

class CircePipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new CirceSerialization[IO, Test]
  val dser = new CirceDeserialization[IO, Test]

  test("jackson identity") {
    data.through(ser.serialize).through(dser.deserialize).compile.toList === list
  }
}
