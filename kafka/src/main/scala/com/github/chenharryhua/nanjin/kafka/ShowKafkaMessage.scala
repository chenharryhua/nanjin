package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Show
import cats.implicits._
import fs2.kafka.{CommittableConsumerRecord => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

private[kafka] trait LowerPriorityShow extends BitraverseFs2Message {
  protected def buildCR[K, V](
    t: ConsumerRecord[K, V],
    key: Option[String],
    value: Option[String]): String = {
    val (utc, local) = utils.kafkaTimestamp(t.timestamp())
    s"""
       |consumer record:
       |topic:        ${t.topic()}
       |partition:    ${t.partition()}
       |offset:       ${t.offset()}
       |timestamp:    ${t.timestamp()}
       |utc:          $utc
       |local-time:   $local
       |ts-type:      ${t.timestampType()}
       |key:          ${key.getOrElse("null")}
       |value:        ${value.getOrElse("null")}
       |key-size:     ${t.serializedKeySize()}
       |value-size:   ${t.serializedValueSize()}
       |headers:      ${t.headers()}
       |leader epoch: ${t.leaderEpoch}""".stripMargin
  }
  protected def buildPR[K, V](
    t: ProducerRecord[K, V],
    key: Option[String],
    value: Option[String]): String = {
    val (utc, local) = utils.kafkaTimestamp(t.timestamp())
    s"""
       |producer record:
       |topic:      ${t.topic}
       |partition:  ${t.partition}
       |timestamp:  ${t.timestamp()}
       |utc:        $utc
       |local-time: $local
       |key:        ${key.getOrElse("null")}
       |value:      ${value.getOrElse("null")}
       |headers:    ${t.headers}""".stripMargin
  }

  implicit protected def showConsumerRecords2[K, V]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) =>
      buildCR(t, Option(t.key).map(_.toString), Option(t.value).map(_.toString))

  implicit protected def showProducerRecord2[K, V]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) =>
      buildPR(t, Option(t.key).map(_.toString), Option(t.value).map(_.toString))

  implicit protected def showFs2CommittableMessage2[F[_], K, V]
    : Show[Fs2CommittableMessage[F, K, V]] =
    (t: Fs2CommittableMessage[F, K, V]) => isoFs2ComsumerRecord.get(t.record).show

  implicit protected def showAkkaCommittableMessage2[K, V]: Show[AkkaCommittableMessage[K, V]] =
    (t: AkkaCommittableMessage[K, V]) => t.record.show
}

private[kafka] trait LowPriorityShow extends LowerPriorityShow {
  implicit protected def showConsumerRecords1[K, V: Show]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) =>
      buildCR(t, Option(t.key).map(_.toString), Option(t.value).map(_.show))

  implicit protected def showProducerRecord1[K, V: Show]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) =>
      buildPR(t, Option(t.key).map(_.toString), Option(t.value).map(_.show))

  implicit protected def showFs2CommittableMessage1[F[_], K, V: Show]
    : Show[Fs2CommittableMessage[F, K, V]] =
    (t: Fs2CommittableMessage[F, K, V]) => isoFs2ComsumerRecord.get(t.record).show

  implicit protected def showAkkaCommittableMessage1[K, V: Show]
    : Show[AkkaCommittableMessage[K, V]] =
    (t: AkkaCommittableMessage[K, V]) => t.record.show
}

trait ShowKafkaMessage extends LowPriorityShow {

  implicit protected def showConsumerRecords[K: Show, V: Show]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) => buildCR(t, Option(t.key).map(_.show), Option(t.value).map(_.show))

  implicit protected def showProducerRecord[K: Show, V: Show]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) => buildPR(t, Option(t.key).map(_.show), Option(t.value).map(_.show))

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
