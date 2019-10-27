package mtest

import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.scalatest.funsuite.AnyFunSuite

class MorpheusNeo4jTest extends AnyFunSuite {
  test("morpheus") {
    val m = morpheus.session
    m.registerSource(Namespace("Neo4j"), morpheus.source)

    val res: RelationalCypherResult[SparkTable.DataFrameTable] =
      m.cypher("""From Neo4j.graph MATCH (n:Person) RETURN n LIMIT 25""")
    res.show
  }
}
