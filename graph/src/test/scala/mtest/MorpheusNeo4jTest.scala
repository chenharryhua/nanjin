package mtest

import cats.effect.IO
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.scalatest.funsuite.AnyFunSuite

class MorpheusNeo4jTest extends AnyFunSuite {
  test("morpheus") {
    val res = morpheus.cypher[IO]("MATCH (n1)-[r]->(n2) RETURN r, n1, n2 LIMIT 25")
    res.unsafeRunSync.show
  }
}
