package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import shapeless.cachedImplicit

final class KafkaAvroCodec[K, V](val keyCodec: AvroCodec[K], val valCodec: AvroCodec[V])
    extends Serializable {
  implicit private val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
  implicit private val schemaForVal: SchemaFor[V] = valCodec.schemaFor
  implicit private val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
  implicit private val valDecoder: Decoder[V]     = valCodec.avroDecoder
  implicit private val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
  implicit private val valEncoder: Encoder[V]     = valCodec.avroEncoder

  // optional key/value
  val schemaForOptionalKV: SchemaFor[OptionalKV[K, V]] = cachedImplicit
  val optionalKVDecoder: Decoder[OptionalKV[K, V]]     = cachedImplicit
  val optionalKVEncoder: Encoder[OptionalKV[K, V]]     = cachedImplicit

  val optionalKVCodec: AvroCodec[OptionalKV[K, V]] =
    AvroCodec[OptionalKV[K, V]](schemaForOptionalKV, optionalKVDecoder, optionalKVEncoder)

  // compulsory key
  val schemaForCompulsoryK: SchemaFor[CompulsoryK[K, V]] = cachedImplicit
  val compulsoryKDecoder: Decoder[CompulsoryK[K, V]]     = cachedImplicit
  val compulsoryKEncoder: Encoder[CompulsoryK[K, V]]     = cachedImplicit

  val compulsoryKCodec: AvroCodec[CompulsoryK[K, V]] =
    AvroCodec[CompulsoryK[K, V]](schemaForCompulsoryK, compulsoryKDecoder, compulsoryKEncoder)

  // compulsory val
  val schemaForCompulsoryV: SchemaFor[CompulsoryV[K, V]] = cachedImplicit
  val compulsoryVDecoder: Decoder[CompulsoryV[K, V]]     = cachedImplicit
  val compulsoryVEncoder: Encoder[CompulsoryV[K, V]]     = cachedImplicit

  val compulsoryVCodec: AvroCodec[CompulsoryV[K, V]] =
    AvroCodec[CompulsoryV[K, V]](schemaForCompulsoryV, compulsoryVDecoder, compulsoryVEncoder)

  // compulsory key/value
  val schemaForCompulsoryKV: SchemaFor[CompulsoryKV[K, V]] = cachedImplicit
  val compulsoryKVDecoder: Decoder[CompulsoryKV[K, V]]     = cachedImplicit
  val compulsoryKVEncoder: Encoder[CompulsoryKV[K, V]]     = cachedImplicit

  val compulsoryKVCodec: AvroCodec[CompulsoryKV[K, V]] =
    AvroCodec[CompulsoryKV[K, V]](schemaForCompulsoryKV, compulsoryKVDecoder, compulsoryKVEncoder)

  // producer record
  val schemaForProducerRecord: SchemaFor[NJProducerRecord[K, V]] = cachedImplicit
  val producerRecordDecoder: Decoder[NJProducerRecord[K, V]]     = cachedImplicit
  val producerRecordEncoder: Encoder[NJProducerRecord[K, V]]     = cachedImplicit

  val producerRecordCodec: AvroCodec[NJProducerRecord[K, V]] =
    AvroCodec[NJProducerRecord[K, V]](
      schemaForProducerRecord,
      producerRecordDecoder,
      producerRecordEncoder)
}

final class KafkaAvroTypedEncoder[K, V](
  val keyEncoder: TypedEncoder[K],
  val valEncoder: TypedEncoder[V],
  val codec: KafkaAvroCodec[K, V])
    extends Serializable {
  implicit private val ke: TypedEncoder[K] = keyEncoder
  implicit private val ve: TypedEncoder[V] = valEncoder

  val optionalKV: TypedEncoder[OptionalKV[K, V]]           = cachedImplicit
  val compulsoryK: TypedEncoder[CompulsoryK[K, V]]         = cachedImplicit
  val compulsoryV: TypedEncoder[CompulsoryV[K, V]]         = cachedImplicit
  val compulsoryKV: TypedEncoder[CompulsoryKV[K, V]]       = cachedImplicit
  val producerRecord: TypedEncoder[NJProducerRecord[K, V]] = cachedImplicit

  val ateOptionalKV: AvroTypedEncoder[OptionalKV[K, V]] =
    AvroTypedEncoder[OptionalKV[K, V]](codec.optionalKVCodec)(optionalKV)

  val ateCompulsoryK: AvroTypedEncoder[CompulsoryK[K, V]] =
    AvroTypedEncoder[CompulsoryK[K, V]](codec.compulsoryKCodec)(compulsoryK)

  val ateCompulsoryV: AvroTypedEncoder[CompulsoryV[K, V]] =
    AvroTypedEncoder[CompulsoryV[K, V]](codec.compulsoryVCodec)(compulsoryV)

  val ateCompulsoryKV: AvroTypedEncoder[CompulsoryKV[K, V]] =
    AvroTypedEncoder[CompulsoryKV[K, V]](codec.compulsoryKVCodec)(compulsoryKV)

  val ateProducerRecord: AvroTypedEncoder[NJProducerRecord[K, V]] =
    AvroTypedEncoder[NJProducerRecord[K, V]](codec.producerRecordCodec)(producerRecord)
}
