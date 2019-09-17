package com.github.chenharryhua.nanjin.codec

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

sealed abstract class CodecException(msg: String)
    extends Exception(msg) with Product with Serializable

object CodecException {
  final case class EncodeException(topic: String, error: Throwable, data: String, schema: Schema)
      extends CodecException(s"""|encode avro failure: 
                                 |topic:    $topic
                                 |error:    ${error.getMessage}
                                 |cause:    ${error.getCause}
                                 |data:     $data
                                 |schema:   ${schema.toString()}""".stripMargin)

  final case object DecodingNullException extends CodecException("decoding null")

  final case class CorruptedRecordException(topic: String, error: Throwable, schema: Schema)
      extends CodecException(s"""|decode avro failure:
                                 |topic:    $topic
                                 |error:    ${error.getMessage}
                                 |cause:    ${error.getCause}
                                 |schema:   ${schema.toString}""".stripMargin)

  final case class InvalidObjectException(
    topic: String,
    error: Throwable,
    genericRecord: GenericRecord,
    schema: Schema)
      extends CodecException(s"""|decode avro failure:
                                 |topic:         $topic
                                 |error:         ${error.getMessage}
                                 |cause:         ${error.getCause}
                                 |GenericRecord: ${genericRecord.toString}
                                 |schema:        ${schema.toString}""".stripMargin)

  final case class DecodingJsonException(msg: String) extends CodecException(msg)
}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable)
    extends Exception(ex.getMessage)
