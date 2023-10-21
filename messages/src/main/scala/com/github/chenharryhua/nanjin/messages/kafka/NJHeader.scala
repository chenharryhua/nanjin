package com.github.chenharryhua.nanjin.messages.kafka

import cats.implicits.{catsSyntaxEq, toShow}
import cats.{Eq, Show}
import com.sksamuel.avro4s.{AvroName, AvroNamespace}
import fs2.kafka.Header
import io.circe.generic.JsonCodec
import io.scalaland.chimney.Transformer
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeader

@JsonCodec
@AvroName("header")
@AvroNamespace("nanjin.kafka")
final case class NJHeader(key: String, value: Array[Byte])
object NJHeader {
  // consistent with fs2.kafka
  implicit val showNJHeader: Show[NJHeader] = (a: NJHeader) => Header(a.key, a.value).show
  implicit val eqNJHeader: Eq[NJHeader] = (x: NJHeader, y: NJHeader) =>
    Header(x.key, x.value) === Header(y.key, y.value)

  implicit val transformerHeaderNJFs2: Transformer[NJHeader, Header] =
    (src: NJHeader) => Header(src.key, src.value)

  implicit val transformHeaderFs2NJ: Transformer[Header, NJHeader] =
    (src: Header) => NJHeader(src.key(), src.value())

  implicit val transformHeaderJavaNJ: Transformer[JavaHeader, NJHeader] =
    (src: JavaHeader) => NJHeader(src.key(), src.value())

  implicit val transformHeaderNJJava: Transformer[NJHeader, JavaHeader] =
    (src: NJHeader) => new RecordHeader(src.key, src.value)
}
