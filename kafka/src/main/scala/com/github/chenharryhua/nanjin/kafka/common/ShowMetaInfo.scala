package com.github.chenharryhua.nanjin.kafka.common

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

trait ShowMetaInfo[A] {
  def metaInfo(a: A): String
}

object ShowMetaInfo {
  def apply[A](implicit ev: ShowMetaInfo[A]): ShowMetaInfo[A] = ev

  implicit def kafkaConsumerRecordHasMetaInfo[K, V]: ShowMetaInfo[ConsumerRecord[K, V]] =
    (a: ConsumerRecord[K, V]) =>
      s"partition=${a.partition()}, offset=${a
        .offset()}, timestamp=${NJTimestamp(a.timestamp())}, topic=${a.topic()}"

  implicit def kafkaProducerRecordHasMetaInfo[K, V]: ShowMetaInfo[ProducerRecord[K, V]] =
    (a: ProducerRecord[K, V]) =>
      s"""partition=${a.partition()}, timestamp=${Option(a.timestamp())
        .map(NJTimestamp(_))}, topic=${a.topic()}"""

  implicit def njConsumerRecordHasMetaInfo[K, V]: ShowMetaInfo[NJConsumerRecord[K, V]] =
    (a: NJConsumerRecord[K, V]) =>
      s"partition=${a.partition}, offset=${a.offset}, timestamp=${a.njTimestamp}, topic=${a.topic}"

  implicit def njProducerRecordHasMetaInfo[K, V]: ShowMetaInfo[NJProducerRecord[K, V]] =
    (a: NJProducerRecord[K, V]) => s"""partition=${a.partition}, timestamp=${a.njTimestamp}"""

}
