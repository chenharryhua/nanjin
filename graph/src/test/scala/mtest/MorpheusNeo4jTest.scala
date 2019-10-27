package mtest

import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.scalatest.funsuite.AnyFunSuite

class MorpheusNeo4jTest extends AnyFunSuite {
  test("connect local neo4j") {
//    morpheus.session.registerSource(Namespace("Neo4j"), morpheus.source)
    val res: RelationalCypherResult[SparkTable.DataFrameTable] =
      morpheus.session.cypher(s"""MATCH (n:Database) RETURN n LIMIT 25""".stripMargin)
    res.show
  }
}
