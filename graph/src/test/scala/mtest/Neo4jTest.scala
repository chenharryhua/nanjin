package mtest

import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.scalatest.funsuite.AnyFunSuite

class Neo4jTest extends AnyFunSuite {
  test("connect local neo4j") {
    neo4j.morpheusSession.registerSource(Namespace("Neo4j"), neo4j.morpheusSource)
    val res: RelationalCypherResult[SparkTable.DataFrameTable] =
      neo4j.morpheusSession.cypher(s"""
                                      |CATALOG CREATE GRAPH Neo4j.products {
                                      |  FROM GRAPH CSV.products RETURN GRAPH
                                      |}
     """.stripMargin)
    res.show
  }
}
