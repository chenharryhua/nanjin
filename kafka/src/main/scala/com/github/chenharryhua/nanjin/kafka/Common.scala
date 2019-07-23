package com.github.chenharryhua.nanjin.kafka

final case class KafkaTopicName(value: String) extends AnyVal

sealed abstract class CodecException(msg: String)
    extends Exception(msg) with Product with Serializable

object CodecException {

  final case class DecodingNullKeyException(topic: String)
      extends CodecException(s"decoding null key in $topic")

  final case class DecodingNullValueException(topic: String)
      extends CodecException(s"decoding null value in $topic")

}
