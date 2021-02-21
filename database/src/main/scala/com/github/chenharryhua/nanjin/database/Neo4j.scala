package com.github.chenharryhua.nanjin.database

import monocle.macros.Lenses
import neotypes.{Async, Driver, GraphDatabase}
import org.neo4j.driver.Config.ConfigBuilder
import org.neo4j.driver.{AuthToken, AuthTokens, Config}

@Lenses final case class Neo4j(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  configBuilder: ConfigBuilder = Config.builder()
) {

  def withConfigUpdate(f: ConfigBuilder => ConfigBuilder): Neo4j =
    Neo4j.configBuilder.modify(f)(this)

  private val connStr: ConnectionString = ConnectionString(Protocols.Neo4j.url(host, Some(port)))
  private val auth: AuthToken           = AuthTokens.basic(username.value, password.value)

  def driver[F[_]](implicit F: Async.Aux[F, F]): F[Driver[F]] =
    GraphDatabase.driver[F](connStr.value, auth, configBuilder.build())

}
