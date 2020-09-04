package com.github.chenharryhua.nanjin.kafka

import java.time.ZoneId

import akka.kafka.ConsumerMessage.CommittableMessage
import cats.Show
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka._
import com.sksamuel.avro4s.Record
import fs2.kafka.CommittableConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

private[kafka] trait ShowKafkaMessage {
  private val zoneId: ZoneId = ZoneId.systemDefault()

  implicit def showConsumerRecord[K, V]: Show[ConsumerRecord[K, V]] =
    (t: ConsumerRecord[K, V]) => {
      val ts = NJTimestamp(t.timestamp())
      s"""
         |consumer record:
         |topic:        ${t.topic()}
         |partition:    ${t.partition()}
         |offset:       ${t.offset()}
         |local-time:   ${ts.atZone(zoneId)}
         |key:          ${Option(t.key).map(_.toString).getOrElse("null")}
         |value:        ${Option(t.value).map(_.toString).getOrElse("null")}
         |key-size:     ${t.serializedKeySize()}
         |value-size:   ${t.serializedValueSize()}
         |ts-type:      ${t.timestampType()}
         |timestamp:    ${t.timestamp()}
         |utc:          ${ts.utc}
         |headers:      ${t.headers()}
         |leader epoch: ${t.leaderEpoch}""".stripMargin
    }

  implicit def showProducerRecord[K, V]: Show[ProducerRecord[K, V]] =
    (t: ProducerRecord[K, V]) => {
      val ts = NJTimestamp(t.timestamp())
      s"""
         |producer record:
         |topic:      ${t.topic}
         |partition:  ${t.partition}
         |local-time: ${ts.atZone(zoneId)}
         |key:        ${Option(t.key).map(_.toString).getOrElse("null")}
         |value:      ${Option(t.value).map(_.toString).getOrElse("null")}
         |timestamp:  ${t.timestamp()}
         |utc:        ${ts.utc}
         |headers:    ${t.headers}""".stripMargin
    }

  implicit def showFs2CommittableMessage[F[_], K, V]: Show[CommittableConsumerRecord[F, K, V]] =
    (t: CommittableConsumerRecord[F, K, V]) => isoFs2ComsumerRecord.get(t.record).show

  implicit def showAkkaCommittableMessage[K, V]: Show[CommittableMessage[K, V]] =
    (t: CommittableMessage[K, V]) => t.record.show

  implicit val showArrayByte: Show[Array[Byte]] = _ => "Array[Byte]"

  implicit def showRecordMetadata: Show[RecordMetadata] = { t: RecordMetadata =>
    val ts = NJTimestamp(t.timestamp())
    s"""
       |topic:     ${t.topic()}
       |partition: ${t.partition()}
       |offset:    ${t.offset()}
       |timestamp: ${t.timestamp()}
       |utc:       ${ts.utc}
       |local:     ${ts.atZone(zoneId)}
       |""".stripMargin
  }

  implicit val showAvroRecord: Show[Record] = _.toString
}
