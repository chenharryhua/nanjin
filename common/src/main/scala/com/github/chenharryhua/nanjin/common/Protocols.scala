package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class WebProtocols(val value: String) extends EnumEntry with Serializable

object WebProtocols extends Enum[WebProtocols] with CatsEnum[WebProtocols] {
  override val values: immutable.IndexedSeq[WebProtocols] = findValues

  case object HTTP extends WebProtocols("HTTP")
  case object HTTPS extends WebProtocols("HTTPS")
  case object FTP extends WebProtocols("ftp")

  type HTTP  = HTTP.type
  type HTTPS = HTTPS.type
  type FTP   = FTP.type
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

  case object MongoDB extends WebProtocols("mongodb")
  case object Postgres extends WebProtocols("jdbc:postgresql")
  case object Redshift extends WebProtocols("jdbc:redshift")
  case object SqlServer extends WebProtocols("jdbc:sqlserver")
  case object Bolt extends WebProtocols("bolt")

  type MongoDB   = MongoDB.type
  type Postgres  = Postgres.type
  type Redshift  = Redshift.type
  type SqlServer = SqlServer.type
  type Bolt      = Bolt.type
}

sealed abstract class S3Protocols(val value: String) extends EnumEntry with Serializable

object S3Protocols extends Enum[S3Protocols] with CatsEnum[S3Protocols] {
  override val values: immutable.IndexedSeq[S3Protocols] = findValues

  case object S3 extends WebProtocols("S3")
  case object S3A extends WebProtocols("S3A")

  type S3  = S3.type
  type S3A = S3A.type
}
