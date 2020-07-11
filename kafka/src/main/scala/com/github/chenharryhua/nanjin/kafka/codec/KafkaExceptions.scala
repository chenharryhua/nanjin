package com.github.chenharryhua.nanjin.kafka.codec

import org.apache.avro.generic.GenericRecord

sealed abstract class CodecException(msg: String)
    extends Exception(msg) with Product with Serializable

object CodecException {

  final case class EncodeException(topic: String, error: Throwable, data: String)
      extends CodecException(s"""|encode avro failure: 
                                 |topic:    $topic
                                 |error:    ${error.getMessage}
                                 |cause:    ${error.getCause}
                                 |data:     $data""".stripMargin)

  final case object DecodingNullException extends CodecException("decoding null")

  final case class CorruptedRecordException(topic: String, error: Throwable)
      extends CodecException(s"""|decode avro failure:
                                 |topic:    $topic
                                 |error:    ${error.getMessage}
                                 |cause:    ${error.getCause}""".stripMargin)

  final case class InvalidObjectException(
    topic: String,
    error: Throwable,
    genericRecord: GenericRecord)
      extends CodecException(s"""|decode avro failure:
                                 |topic:         $topic
                                 |error:         ${error.getMessage}
                                 |cause:         ${error.getCause}
                                 |GenericRecord: ${genericRecord.toString}
                                 |
                                 |Most likely the schem includes non-standard Logical Type""".stripMargin)

  final case class DecodingJsonException(msg: String) extends CodecException(msg)
}
