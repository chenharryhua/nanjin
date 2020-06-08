package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{
  GenericRecordDeserialization,
  GenericRecordSerialization
}
import org.scalatest.funsuite.AnyFunSuite

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new GenericRecordSerialization[IO, Test]
  val dser = new GenericRecordDeserialization[IO, Test]

  test("binary identity") {
    data.through(ser.serialize).through(dser.deserialize).compile.toList === list
  }
}
