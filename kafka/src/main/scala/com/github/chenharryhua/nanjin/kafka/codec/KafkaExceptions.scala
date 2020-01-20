package com.github.chenharryhua.nanjin.kafka.codec

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

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
                                 |schema:        ${schema.toString}
                                 |
                                 |Most likely the schem includes non-standard Logical Type""".stripMargin)

  final case class DecodingJsonException(msg: String) extends CodecException(msg)
}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable)
    extends Exception(ex.getMessage)

final case class ConsumerRecordError(
  error: Throwable,
  tag: KeyValueTag,
  topicName: String,
  partition: Int,
  offset: Long)

object ConsumerRecordError {

  def apply[K, V](ex: Throwable, tag: KeyValueTag, cr: ConsumerRecord[K, V]): ConsumerRecordError =
    ConsumerRecordError(ex, tag, cr.topic(), cr.partition(), cr.offset())
}
