package mtest.database

import cats.effect.IO
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global

class DatabaseTest extends AnyFunSuite {
  test("gen case class") {
    println(postgres.genCaseClass[IO].unsafeRunSync())
  }
}
