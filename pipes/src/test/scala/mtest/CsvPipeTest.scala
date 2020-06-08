package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{CsvDeserialization, CsvSerialization}
import kantan.csv._
import org.scalatest.funsuite.AnyFunSuite
import kantan.csv.generic._ 
class CsvPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new CsvSerialization[IO, Test](CsvConfiguration.rfc)
  val dser = new CsvDeserialization[IO, Test](CsvConfiguration.rfc)

  test("csv identity") {
    data.through(ser.serialize).through(dser.deserialize).compile.toList === list
  }
}
