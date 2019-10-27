package mtest

import cats.effect.{ExitCode, IO}
import fs2.Stream
import neotypes.DeferredQueryBuilder
import neotypes.implicits.syntax.cypher._
import org.scalatest.funsuite.AnyFunSuite
import neotypes.cats.effect.implicits._
import neotypes.implicits.mappers.results._
import neotypes.fs2.implicits._
import cats.derived.auto.show._
import cats.implicits._

final case class Person(name: String)

class NeotypesTests extends AnyFunSuite {

  val person = c"""MATCH (n:Person) RETURN n LIMIT 25"""

  test("neotypes") {
    ntSession
      .sessionStream[IO]
      .flatMap(s => person.query[Person].stream[Stream[IO, *]](s))
      .showLinesStdOut
      .compile
      .drain
      .unsafeRunSync
  }
}
