package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  NJProducerRecord,
  OptionalKV
}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import shapeless.cachedImplicit

final class KafkaAvroCodec[K, V](val keyCodec: NJAvroCodec[K], val valCodec: NJAvroCodec[V])
    extends Serializable {
  implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
  implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
  implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
  implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
  implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
  implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder

  // optional key/value
  val schemaForOptionalKV: SchemaFor[OptionalKV[K, V]] = cachedImplicit
  val optionalKVDecoder: Decoder[OptionalKV[K, V]]     = cachedImplicit
  val optionalKVEncoder: Encoder[OptionalKV[K, V]]     = cachedImplicit

  val optionalKVCodec: NJAvroCodec[OptionalKV[K, V]] =
    NJAvroCodec[OptionalKV[K, V]](schemaForOptionalKV, optionalKVDecoder, optionalKVEncoder)

  // compulsory key
  val schemaForCompulsoryK: SchemaFor[CompulsoryK[K, V]] = cachedImplicit
  val compulsoryKDecoder: Decoder[CompulsoryK[K, V]]     = cachedImplicit
  val compulsoryKEncoder: Encoder[CompulsoryK[K, V]]     = cachedImplicit

  val compulsoryKCodec: NJAvroCodec[CompulsoryK[K, V]] =
    NJAvroCodec[CompulsoryK[K, V]](schemaForCompulsoryK, compulsoryKDecoder, compulsoryKEncoder)

  // compulsory val
  val schemaForCompulsoryV: SchemaFor[CompulsoryV[K, V]] = cachedImplicit
  val compulsoryVDecoder: Decoder[CompulsoryV[K, V]]     = cachedImplicit
  val compulsoryVEncoder: Encoder[CompulsoryV[K, V]]     = cachedImplicit

  val compulsoryVCodec: NJAvroCodec[CompulsoryV[K, V]] =
    NJAvroCodec[CompulsoryV[K, V]](schemaForCompulsoryV, compulsoryVDecoder, compulsoryVEncoder)

  // compulsory key/value
  val schemaForCompulsoryKV: SchemaFor[CompulsoryKV[K, V]] = cachedImplicit
  val compulsoryKVDecoder: Decoder[CompulsoryKV[K, V]]     = cachedImplicit
  val compulsoryKVEncoder: Encoder[CompulsoryKV[K, V]]     = cachedImplicit

  val compulsoryKVCodec: NJAvroCodec[CompulsoryKV[K, V]] =
    NJAvroCodec[CompulsoryKV[K, V]](schemaForCompulsoryKV, compulsoryKVDecoder, compulsoryKVEncoder)

}

final class KafkaAvroTypedEncoder[K, V](
  val keyEncoder: TypedEncoder[K],
  val valEncoder: TypedEncoder[V],
  val codec: KafkaAvroCodec[K, V])
    extends Serializable {
  implicit private val ke: TypedEncoder[K]                          = keyEncoder
  implicit private val ve: TypedEncoder[V]                          = valEncoder
  implicit val optionalKV: TypedEncoder[OptionalKV[K, V]]           = cachedImplicit
  implicit val compulsoryK: TypedEncoder[CompulsoryK[K, V]]         = cachedImplicit
  implicit val compulsoryV: TypedEncoder[CompulsoryV[K, V]]         = cachedImplicit
  implicit val compulsoryKV: TypedEncoder[CompulsoryKV[K, V]]       = cachedImplicit
  implicit val producerRecord: TypedEncoder[NJProducerRecord[K, V]] = cachedImplicit

  val ateOptionalKV: AvroTypedEncoder[OptionalKV[K, V]] =
    AvroTypedEncoder[OptionalKV[K, V]](codec.optionalKVCodec)

  val ateCompulsoryK: AvroTypedEncoder[CompulsoryK[K, V]] =
    AvroTypedEncoder[CompulsoryK[K, V]](codec.compulsoryKCodec)

  val ateCompulsoryV: AvroTypedEncoder[CompulsoryV[K, V]] =
    AvroTypedEncoder[CompulsoryV[K, V]](codec.compulsoryVCodec)

  val ateCompulsoryKV: AvroTypedEncoder[CompulsoryKV[K, V]] =
    AvroTypedEncoder[CompulsoryKV[K, V]](codec.compulsoryKVCodec)
}
