package com.github.chenharryhua.nanjin.graph

import cats.effect.{Async, Resource}
import com.github.chenharryhua.nanjin.database._
import fs2.Stream
import monocle.macros.Lenses
import neotypes.cats.effect.implicits.catsAsync
import neotypes.{GraphDatabase, Session, Transaction}
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.Config.ConfigBuilder
import org.neo4j.driver.v1.{AuthToken, AuthTokens, Config}
import org.opencypher.morpheus.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.okapi.neo4j.io.Neo4jConfig

@Lenses final case class Neo4j(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  configBuilder: ConfigBuilder = Config.builder()) {

  def updateConfig(f: ConfigBuilder => ConfigBuilder): Neo4j = Neo4j.configBuilder.modify(f)(this)

  val connStr: ConnectionString = ConnectionString(s"bolt://${host.value}:${port.value}")
  val auth: AuthToken           = AuthTokens.basic(username.value, password.value)

}

final case class Neo4jSession(config: Neo4j, spark: SparkSession) {

  // neotypes
  def sessionResource[F[_]: Async]: Resource[F, Session[F]] =
    for {
      driver <- GraphDatabase
        .driver[F](config.connStr.value, config.auth, config.configBuilder.build())
      session <- driver.session
    } yield session

  def transactionResource[F[_]: Async]: Resource[F, Transaction[F]] =
    sessionResource.evalMap(_.transaction)

  def sessionStream[F[_]: Async]: Stream[F, Session[F]] =
    Stream.resource(sessionResource)

  def transactionStream[F[_]: Async]: Stream[F, Transaction[F]] =
    Stream.resource(transactionResource)

  // morpheus
  private val morpheusConfig: Neo4jConfig =
    Neo4jConfig(config.connStr.uri, config.username.value, Some(config.password.value))

  implicit val morpheusSession: MorpheusSession    = MorpheusSession.create(spark)
  def morpheusSource: Neo4jPropertyGraphDataSource = GraphSources.cypher.neo4j(morpheusConfig)
}
