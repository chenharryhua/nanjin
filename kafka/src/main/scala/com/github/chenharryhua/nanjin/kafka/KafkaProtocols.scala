package com.github.chenharryhua.nanjin.kafka

import enumeratum.values.{CatsValueEnum, ShortEnum, ShortEnumEntry}
import monocle.Iso
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.immutable

sealed abstract private[kafka] class KafkaProtocols private (val value: Short, val name: String)
    extends ShortEnumEntry with Serializable {
  final def toSecurityProtocol: SecurityProtocol = SecurityProtocol.forId(value)
}

private[kafka] object KafkaProtocols
    extends CatsValueEnum[Short, KafkaProtocols] with ShortEnum[KafkaProtocols] {
  override val values: immutable.IndexedSeq[KafkaProtocols] = findValues

  case object PLAINTEXT extends KafkaProtocols(0, SecurityProtocol.PLAINTEXT.name)
  case object SSL extends KafkaProtocols(1, SecurityProtocol.SSL.name)
  case object SASL_PLAINTEXT extends KafkaProtocols(2, SecurityProtocol.SASL_PLAINTEXT.name)
  case object SASL_SSL extends KafkaProtocols(3, SecurityProtocol.SASL_SSL.name)

  type SASL_SSL       = SASL_SSL.type
  type PLAINTEXT      = PLAINTEXT.type
  type SASL_PLAINTEXT = SASL_PLAINTEXT.type
  type SSL            = SSL.type

  def apply(sp: SecurityProtocol): KafkaProtocols =
    KafkaProtocols.withValue(sp.id)

  implicit val isoSecurityProtocol: Iso[KafkaProtocols, SecurityProtocol] =
    Iso[KafkaProtocols, SecurityProtocol](_.toSecurityProtocol)(KafkaProtocols.apply)
}
