package com.github.chenharryhua.nanjin.kafka.codec

import com.github.chenharryhua.nanjin.kafka.ShowMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.KeyValueTag
import monocle.macros.Lenses
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

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

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable)
    extends Exception(ex.getMessage)
final case class KafkaStreamingException(msg: String) extends Exception(msg)

@Lenses final case class ConsumerRecordError(error: Throwable, tag: KeyValueTag, metaInfo: String) {

  def valueError: Option[ConsumerRecordError] =
    ConsumerRecordError.tag.composePrism(KeyValueTag.valueTagPrism).getOption(this).map(_ => this)

  def keyError: Option[ConsumerRecordError] =
    ConsumerRecordError.tag.composePrism(KeyValueTag.keyTagPrism).getOption(this).map(_ => this)
}

object ConsumerRecordError {

  def apply[K, V](ex: Throwable, tag: KeyValueTag, cr: ConsumerRecord[K, V]): ConsumerRecordError =
    ConsumerRecordError(ex, tag, s"${tag.name}  ${ShowMetaInfo[ConsumerRecord[K, V]].metaInfo(cr)}")
}
