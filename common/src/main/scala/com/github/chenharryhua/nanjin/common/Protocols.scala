package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class HttpProtocols(val value: String) extends EnumEntry with Serializable

object HttpProtocols extends Enum[HttpProtocols] with CatsEnum[HttpProtocols] {
  override val values: immutable.IndexedSeq[HttpProtocols] = findValues

  case object HTTP extends HttpProtocols("HTTP")
  case object HTTPS extends HttpProtocols("HTTPS")

  type HTTP  = HTTP.type
  type HTTPS = HTTPS.type
}

sealed abstract class KafkaProtocols(val value: String) extends EnumEntry with Serializable

object KafkaProtocols extends Enum[KafkaProtocols] with CatsEnum[KafkaProtocols] {
  override val values: immutable.IndexedSeq[KafkaProtocols] = findValues
  case object SSL extends KafkaProtocols("SSL")
  case object SASL_SSL extends KafkaProtocols("SASL_SSL")
  case object PLAINTEXT extends KafkaProtocols("PLAINTEXT")
  case object SASL_PLAINTEXT extends KafkaProtocols("SASL_PLAINTEXT")

  type SASL_SSL       = SASL_SSL.type
  type PLAINTEXT      = PLAINTEXT.type
  type SASL_PLAINTEXT = SASL_PLAINTEXT.type
  type SSL            = SSL.type
}

sealed abstract class DatabaseProtocols(val value: String) extends EnumEntry with Serializable

object DatabaseProtocols extends Enum[DatabaseProtocols] with CatsEnum[DatabaseProtocols] {
  override val values: immutable.IndexedSeq[DatabaseProtocols] = findValues

  case object MongoDB extends DatabaseProtocols("mongodb")
  case object Postgres extends DatabaseProtocols("jdbc:postgresql")
  case object Redshift extends DatabaseProtocols("jdbc:redshift")
  case object SqlServer extends DatabaseProtocols("jdbc:sqlserver")
  case object Bolt extends DatabaseProtocols("bolt")

  type MongoDB   = MongoDB.type
  type Postgres  = Postgres.type
  type Redshift  = Redshift.type
  type SqlServer = SqlServer.type
  type Bolt      = Bolt.type
}

sealed abstract class S3Protocols(val value: String) extends EnumEntry with Serializable

object S3Protocols extends Enum[S3Protocols] with CatsEnum[S3Protocols] {
  override val values: immutable.IndexedSeq[S3Protocols] = findValues

  case object S3 extends S3Protocols("S3")
  case object S3A extends S3Protocols("S3A")

  type S3  = S3.type
  type S3A = S3A.type
}
