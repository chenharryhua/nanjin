package com.github.chenharryhua.nanjin.kafka

final case class KafkaTopicName(value: String) extends AnyVal

sealed abstract class CodecException(msg: String)
    extends Exception(msg) with Product with Serializable
//encoding
final case class EncodeException(msg: String) extends CodecException(msg)
//decoding
final case class DecodingNullException(topic: String)
    extends CodecException(s"decode null in topic: $topic")
final case class CorruptedRecordException(msg: String) extends CodecException(msg)
final case class DecodingJsonException(msg: String) extends CodecException(msg)
