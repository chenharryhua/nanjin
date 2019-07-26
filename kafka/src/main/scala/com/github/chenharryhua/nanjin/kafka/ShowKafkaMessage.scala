package com.github.chenharryhua.nanjin.kafka

import java.time.{Instant, ZoneId}

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Show
import cats.implicits._
import fs2.kafka.{CommittableMessage => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

trait ShowKafkaMessage {
  private def utc(t: Long): Instant = Instant.ofEpochMilli(t)
  implicit protected def showConsumerRecords[K: Show, V: Show]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) => {
      val dt = utc(t.timestamp())
      s"""
         |consumer record:
         |topic:        ${t.topic()}
         |partition:    ${t.partition()}
         |headers:      ${t.headers()}
         |offset:       ${t.offset()}
         |timestamp:    ${t.timestamp()}
         |utc:          $dt
         |local-time:   ${dt.atZone(ZoneId.systemDefault())}
         |ts-type:      ${t.timestampType()}
         |key:          ${t.key().show}
         |value:        ${t.value().show}
         |key-size:     ${t.serializedKeySize()}
         |value-size:   ${t.serializedValueSize()}
         |leader epoch: ${t.leaderEpoch}""".stripMargin
    }

  implicit protected def showProducerRecord[K: Show, V: Show]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) => {
      val dt = utc(t.timestamp())
      s"""
         |producer record:
         |topic:      ${t.topic}
         |partition:  ${t.partition}
         |headers:    ${t.headers}
         |timestamp:  ${t.timestamp()}
         |utc:        $dt
         |local-time: ${dt.atZone(ZoneId.systemDefault())}
         |key:        ${t.key.show}
         |valu:       ${t.value.show}""".stripMargin
    }
  implicit protected def showFs2CommittableMessage[F[_], K: Show, V: Show]
    : Show[Fs2CommittableMessage[F, K, V]] =
    (t: Fs2CommittableMessage[F, K, V]) => {
      s"""
         |fs2 committable message:
         |${t.record.show}
         |${t.committableOffset}""".stripMargin
    }

  implicit protected def showAkkaCommittableMessage[K: Show, V: Show]
    : Show[AkkaCommittableMessage[K, V]] =
    (t: AkkaCommittableMessage[K, V]) => {
      s"""
         |akka committable message:
         |${t.record.show}
         |${t.committableOffset}
         |""".stripMargin
    }
}
