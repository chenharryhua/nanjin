package com.github.chenharryhua.nanjin.kafka.record

import cats.Eq
import cats.derived.derived
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoHeader
import com.google.protobuf.ByteString
import com.sksamuel.avro4s.{AvroName, AvroNamespace}
import fs2.kafka.Header
import io.circe.Codec
import io.scalaland.chimney.Transformer
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeader

/** A Kafka record header: a key and its bytes.
  *
  * Kafka permits a header whose value is `null`, which this type does not distinguish from a header whose
  * value is empty: converting from a Kafka header maps `null` to `Nil`, and converting back produces an empty
  * array rather than `null`. Round-tripping a null-valued header therefore yields an empty-valued one. The
  * alternative, an optional `value`, would make the serialized `value` field nullable, so the distinction is
  * traded away deliberately to keep the wire format stable.
  */
@AvroName("header")
@AvroNamespace("nanjin.kafka")
final case class NJHeader(key: String, value: List[Byte]) derives Codec.AsObject, Eq
object NJHeader {

  given Transformer[NJHeader, Header] =
    (src: NJHeader) => Header(src.key, src.value.toArray)

  given Transformer[Header, NJHeader] =
    (src: Header) => NJHeader(src.key(), Option(src.value()).fold(List.empty[Byte])(_.toList))

  given Transformer[JavaHeader, NJHeader] =
    (src: JavaHeader) => NJHeader(src.key(), Option(src.value()).fold(List.empty[Byte])(_.toList))

  given Transformer[NJHeader, JavaHeader] =
    (src: NJHeader) => new RecordHeader(src.key, src.value.toArray)

  given Transformer[NJHeader, ProtoHeader] =
    (src: NJHeader) => ProtoHeader(src.key, ByteString.copyFrom(src.value.toArray))
}
