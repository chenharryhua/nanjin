package com.github.chenharryhua.nanjin.messages.kafka

import fs2.kafka.*
import io.scalaland.chimney.dsl.*
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord

object instances extends MessageTransformers with EqMessage {

  given [K, V]: Iso[JavaProducerRecord[K, V], JavaProducerRecord[K, V]] =
    Iso[JavaProducerRecord[K, V], JavaProducerRecord[K, V]](identity)(identity)

  given [K, V]: Iso[JavaConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    Iso[JavaConsumerRecord[K, V], JavaConsumerRecord[K, V]](identity)(identity)

  given [K, V]: Iso[ProducerRecord[K, V], JavaProducerRecord[K, V]] =
    Iso[ProducerRecord[K, V], JavaProducerRecord[K, V]](_.transformInto[JavaProducerRecord[K, V]])(
      _.transformInto[ProducerRecord[K, V]])

  given [K, V]: Iso[ConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    Iso[ConsumerRecord[K, V], JavaConsumerRecord[K, V]](_.transformInto[JavaConsumerRecord[K, V]])(
      _.transformInto[ConsumerRecord[K, V]])

}
