package com.github.chenharryhua.nanjin.graph

import cats.effect.{Async, Resource, Sync}
import fs2.Stream
import neotypes.cats.effect.implicits.catsAsync
import neotypes.{GraphDatabase, Session, Transaction}
import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.neo4j.io.{MetaLabelSupport, Neo4jConfig}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

final case class NeotypesSession(settings: Neo4jSettings) {

  def sessionResource[F[_]: Async]: Resource[F, Session[F]] =
    for {
      driver <- GraphDatabase
        .driver[F](settings.connStr.value, settings.auth, settings.configBuilder.build())
      session <- driver.session
    } yield session

  def transactionResource[F[_]: Async]: Resource[F, Transaction[F]] =
    sessionResource.evalMap(_.transaction)

  def sessionStream[F[_]: Async]: Stream[F, Session[F]] =
    Stream.resource(sessionResource)

  def transactionStream[F[_]: Async]: Stream[F, Transaction[F]] =
    Stream.resource(transactionResource)
}

final case class MorpheusNeo4jSession(settings: Neo4jSettings, spark: SparkSession) {

  private val config: Neo4jConfig =
    Neo4jConfig(settings.connStr.uri, settings.username.value, Some(settings.password.value))

  implicit val session: MorpheusSession    = MorpheusSession.create(spark)
  val source: Neo4jPropertyGraphDataSource = GraphSources.cypher.neo4j(config)

  def schema: Option[String] = source.schema(MetaLabelSupport.entireGraphName).map(_.toJson)

  def cypher[F[_]: Sync](query: String): F[RelationalCypherResult[SparkTable.DataFrameTable]] =
    Resource
      .make(
        Sync[F].delay(session.registerSource(Namespace("Neo4j"), source))
      )(_ => Sync[F].delay(session.deregisterSource(Namespace("Neo4j"))))
      .use { _ =>
        Sync[F].delay(session.cypher("From Neo4j.graph " + query))
      }

}
