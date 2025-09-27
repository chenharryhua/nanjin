package com.github.chenharryhua.nanjin.messages.kafka

import cats.Eq
import com.sksamuel.avro4s.{AvroName, AvroNamespace}
import fs2.kafka.Header
import io.circe.generic.JsonCodec
import io.scalaland.chimney.Transformer
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeader

@JsonCodec
@AvroName("header")
@AvroNamespace("nanjin.kafka")
final case class NJHeader(key: String, value: List[Byte])
object NJHeader {
  implicit val eqNJHeader: Eq[NJHeader] = cats.derived.semiauto.eq

  implicit val transformerHeaderNJFs2: Transformer[NJHeader, Header] =
    (src: NJHeader) => Header(src.key, src.value.toArray)

  implicit val transformHeaderFs2NJ: Transformer[Header, NJHeader] =
    (src: Header) => NJHeader(src.key(), src.value().toList)

  implicit val transformHeaderJavaNJ: Transformer[JavaHeader, NJHeader] =
    (src: JavaHeader) => NJHeader(src.key(), src.value().toList)

  implicit val transformHeaderNJJava: Transformer[NJHeader, JavaHeader] =
    (src: NJHeader) => new RecordHeader(src.key, src.value.toArray)
}
