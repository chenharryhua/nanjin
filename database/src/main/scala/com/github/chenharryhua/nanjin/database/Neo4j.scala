package com.github.chenharryhua.nanjin.database

import cats.effect.{Async, Resource}
import fs2.Stream
import monocle.macros.Lenses
import neotypes.cats.effect.implicits._
import neotypes.{GraphDatabase, Session, Transaction}
import org.neo4j.driver.v1.Config.ConfigBuilder
import org.neo4j.driver.v1.{AuthToken, AuthTokens, Config}

@Lenses final case class Neo4j(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  configBuilder: ConfigBuilder = Config.builder()
) {

  def withConfigUpdate(f: ConfigBuilder => ConfigBuilder): Neo4j =
    Neo4j.configBuilder.modify(f)(this)

  private val connStr: ConnectionString = ConnectionString(s"bolt://${host.value}:${port.value}")
  private val auth: AuthToken           = AuthTokens.basic(username.value, password.value)

  def sessionResource[F[_]: Async]: Resource[F, Session[F]] =
    for {
      driver <- GraphDatabase.driver[F](connStr.value, auth, configBuilder.build())
      session <- driver.session
    } yield session

  def transactionResource[F[_]: Async]: Resource[F, Transaction[F]] =
    sessionResource.evalMap(_.transaction)

  def sessionStream[F[_]: Async]: Stream[F, Session[F]] =
    Stream.resource(sessionResource)

  def transactionStream[F[_]: Async]: Stream[F, Transaction[F]] =
    Stream.resource(transactionResource)
}
