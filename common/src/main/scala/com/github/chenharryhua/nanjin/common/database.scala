package com.github.chenharryhua.nanjin.common

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.And
import eu.timepit.refined.cats.*
import eu.timepit.refined.collection.{MaxSize, NonEmpty}
import eu.timepit.refined.string.{Trimmed, Uri}
import eu.timepit.refined.types.net
import io.circe.generic.JsonCodec
import io.circe.refined.*

object database {
  type Username = String Refined And[NonEmpty, Trimmed]
  object Username extends RefinedTypeOps[Username, String] with CatsRefinedTypeOpsSyntax

  type Password = String Refined And[NonEmpty, Trimmed]
  object Password extends RefinedTypeOps[Password, String] with CatsRefinedTypeOpsSyntax

  type DatabaseName = String Refined And[NonEmpty, MaxSize[128]]
  object DatabaseName extends RefinedTypeOps[DatabaseName, String] with CatsRefinedTypeOpsSyntax

  type TableName = String Refined And[NonEmpty, MaxSize[128]]
  object TableName extends RefinedTypeOps[TableName, String] with CatsRefinedTypeOpsSyntax

  type Host = String Refined Uri
  object Host extends RefinedTypeOps[Host, String] with CatsRefinedTypeOpsSyntax

  type Port = net.PortNumber
  final val Port = net.PortNumber

  @JsonCodec final case class Postgres(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

  @JsonCodec final case class Redshift(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

  @JsonCodec final case class SqlServer(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

}
