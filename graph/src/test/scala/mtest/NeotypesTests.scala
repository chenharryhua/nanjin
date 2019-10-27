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

final case class Database(name: String)

class NeotypesTests extends AnyFunSuite {

  val query: DeferredQueryBuilder =
    c"""
    CREATE (db:Database {name:"google"})-[r:SAYS]->(msg:Message {name:"no evil"}) RETURN db, msg, r
    """

  val q2 = c"""MATCH (n:Database) RETURN n LIMIT 25"""

  test("neotypes") {
    ntSession
      .sessionStream[IO]
      .flatMap(s => q2.query[Database].stream[Stream[IO, *]](s))
      .showLinesStdOut
      .compile
      .drain
      .unsafeRunSync
  }
}
