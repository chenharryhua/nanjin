package mtest

import cats.effect.IO
import org.scalatest.funsuite.AnyFunSuite

class MorpheusNeo4jTest extends AnyFunSuite {
  test("morpheus") {
    val res = morpheus.cypher[IO]("MATCH (n1)-[r]->(n2) RETURN r, n1, n2 LIMIT 25")
    res.map(_.show).unsafeRunSync()
  }
}
