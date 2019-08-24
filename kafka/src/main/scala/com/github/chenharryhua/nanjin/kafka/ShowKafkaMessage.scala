package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Show
import cats.implicits._
import fs2.kafka.{CommittableConsumerRecord => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

trait ShowKafkaMessage extends BitraverseFs2Message {

  implicit protected def showConsumerRecords[K: Show, V: Show]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) => {
      val (utc, local) = utils.kafkaTimestamp(t.timestamp())
      s"""
         |consumer record:
         |topic:        ${t.topic()}
         |partition:    ${t.partition()}
         |offset:       ${t.offset()}
         |local-time:   $local
         |ts-type:      ${t.timestampType()}
         |key:          ${Option(t.key).map(_.show).getOrElse("null")}
         |value:        ${Option(t.value).map(_.show).getOrElse("null")}
         |key-size:     ${t.serializedKeySize()}
         |value-size:   ${t.serializedValueSize()}
         |timestamp:    ${t.timestamp()}
         |utc:          $utc
         |headers:      ${t.headers()}
         |leader epoch: ${t.leaderEpoch}""".stripMargin
    }

  implicit protected def showProducerRecord[K: Show, V: Show]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) => {
      val (utc, local) = utils.kafkaTimestamp(t.timestamp())
      s"""
         |producer record:
         |topic:      ${t.topic}
         |partition:  ${t.partition}
         |local-time: $local
         |key:        ${Option(t.key).map(_.show).getOrElse("null")}
         |value:      ${Option(t.value).map(_.show).getOrElse("null")}
         |timestamp:  ${t.timestamp()}
         |utc:        $utc
         |headers:    ${t.headers}""".stripMargin
    }

  implicit protected def showFs2CommittableMessage[F[_], K: Show, V: Show]
    : Show[Fs2CommittableMessage[F, K, V]] =
    (t: Fs2CommittableMessage[F, K, V]) => isoFs2ComsumerRecord.get(t.record).show

  implicit protected def showAkkaCommittableMessage[K: Show, V: Show]
    : Show[AkkaCommittableMessage[K, V]] =
    (t: AkkaCommittableMessage[K, V]) => t.record.show

  implicit protected val showArrayByte: Show[Array[Byte]] = _ => "Array[Byte]"

  implicit protected def showRecordMetadata: Show[RecordMetadata] = { t: RecordMetadata =>
    val (utc, local) = utils.kafkaTimestamp(t.timestamp())
    s"""
       |topic:     ${t.topic()}
       |partition: ${t.partition()}
       |offset:    ${t.offset()}
       |timestamp: ${t.timestamp()}
       |utc:       $utc
       |local:     $local
       |""".stripMargin
  }
}
