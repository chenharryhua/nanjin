package com.github.chenharryhua.nanjin.database

import cats.syntax.show.showInterpolator

enum Protocols(val value: String):
  case MongoDB extends Protocols("mongodb")
  case Postgres extends Protocols("jdbc:postgresql")
  case Redshift extends Protocols("jdbc:redshift")
  case SqlServer extends Protocols("jdbc:sqlserver")
  case Neo4j extends Protocols("bolt")

  final def url(host: String, port: Option[Int]): String =
    port match {
      case None       => show"$value://$host"
      case Some(port) => show"$value://$host:$port"
    }
