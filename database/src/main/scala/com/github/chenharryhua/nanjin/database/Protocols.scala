package com.github.chenharryhua.nanjin.database

import cats.syntax.show.showInterpolator

enum Protocols(val value: String):
  case MongoDB extends Protocols("mongodb")
  case Postgres extends Protocols("jdbc:postgresql")
  case Redshift extends Protocols("jdbc:redshift")
  case SqlServer extends Protocols("jdbc:sqlserver")
  case Neo4j extends Protocols("bolt")

  final def url(host: Host, port: Option[Port]): String =
    port match {
      case None    => show"$value://${host.value}"
      case Some(p) => show"$value://${host.value}:${p.value}"
    }
