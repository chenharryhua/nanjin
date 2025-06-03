package com.github.chenharryhua.nanjin.database

import cats.implicits.showInterpolator
import com.github.chenharryhua.nanjin.common.database.*
import enumeratum.{CatsEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract private[database] class Protocols(val value: String)
    extends EnumEntry with Product with Serializable {

  final def url(host: Host, port: Option[Port]): String =
    port match {
      case None    => show"$value://${host.value}"
      case Some(p) => show"$value://${host.value}:${p.value}"
    }
}

private[database] object Protocols extends Enum[Protocols] with CatsEnum[Protocols] {
  override val values: immutable.IndexedSeq[Protocols] = findValues

  case object MongoDB extends Protocols("mongodb")
  case object Postgres extends Protocols("jdbc:postgresql")
  case object Redshift extends Protocols("jdbc:redshift")
  case object SqlServer extends Protocols("jdbc:sqlserver")
  case object Neo4j extends Protocols("bolt")

  type MongoDB = MongoDB.type
  type Postgres = Postgres.type
  type Redshift = Redshift.type
  type SqlServer = SqlServer.type
  type Neo4j = Neo4j.type
}
