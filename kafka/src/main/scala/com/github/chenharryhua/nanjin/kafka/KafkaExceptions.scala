package com.github.chenharryhua.nanjin.kafka

sealed abstract class CodecException(msg: String)
    extends Exception(msg) with Product with Serializable

object CodecException {
  final case class EncodeException(msg: String) extends CodecException(msg)

  final case class DecodingNullKeyException(topic: String)
      extends CodecException(s"decoding null key in $topic")

  final case class DecodingNullValueException(topic: String)
      extends CodecException(s"decoding null value in $topic")

  final case class CorruptedRecordException(msg: String) extends CodecException(msg)
  final case class InvalidObjectException(msg: String) extends CodecException(msg)
  final case class InvalidGenericRecordException(msg: String) extends CodecException(msg)
  final case class DecodingJsonException(msg: String) extends CodecException(msg)
}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable)
    extends Exception(ex.getMessage)
