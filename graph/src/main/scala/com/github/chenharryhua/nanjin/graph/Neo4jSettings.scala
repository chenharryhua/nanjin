package com.github.chenharryhua.nanjin.graph

import com.github.chenharryhua.nanjin.common._
import monocle.macros.Lenses
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.Config.ConfigBuilder
import org.neo4j.driver.v1.{AuthToken, AuthTokens, Config}
import cats.effect.Async

@Lenses final case class Neo4jSettings(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  configBuilder: ConfigBuilder = Config.builder()) {

  def updateConfig(f: ConfigBuilder => ConfigBuilder): Neo4jSettings =
    Neo4jSettings.configBuilder.modify(f)(this)

  val connStr: ConnectionString = ConnectionString(s"bolt://${host.value}:${port.value}")
  val auth: AuthToken           = AuthTokens.basic(username.value, password.value)

  def neotypes[F[_]: Async]: NeotypesSession[F]           = NeotypesSession[F](this)
  def morpheus(spark: SparkSession): MorpheusNeo4jSession = MorpheusNeo4jSession(this, spark)

}
