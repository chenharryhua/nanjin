package mtest.database

import cats.effect.IO
import org.scalatest.funsuite.AnyFunSuite

class DatabaseTest extends AnyFunSuite {
  test("gen case class") {
    println(postgres.genCaseClass[IO].unsafeRunSync())
  }
}
