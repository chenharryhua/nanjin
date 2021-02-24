package com.github.chenharryhua.nanjin.database

import monocle.macros.Lenses
import org.neo4j.driver.Config.ConfigBuilder
import org.neo4j.driver.{AuthToken, AuthTokens, Config}

@Lenses final case class Neo4j(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  configBuilder: ConfigBuilder = Config.builder()
) {

  def withUsername(un: String): Neo4j  = Neo4j.username.set(Username.unsafeFrom(un))(this)
  def withPassword(pwd: String): Neo4j = Neo4j.password.set(Password.unsafeFrom(pwd))(this)
  def withHost(h: String): Neo4j       = Neo4j.host.set(Host.unsafeFrom(h))(this)
  def withPort(p: Int): Neo4j          = Neo4j.port.set(Port.unsafeFrom(p))(this)

  def withConfigUpdate(f: ConfigBuilder => ConfigBuilder): Neo4j =
    Neo4j.configBuilder.modify(f)(this)

  val connStr: ConnectionString = ConnectionString(Protocols.Neo4j.url(host, Some(port)))
  val auth: AuthToken           = AuthTokens.basic(username.value, password.value)

//  def driver[F[_]](implicit F: Async.Aux[F, F]): F[Driver[F]] =
//    GraphDatabase.driver[F](connStr.value, auth, configBuilder.build())

}
