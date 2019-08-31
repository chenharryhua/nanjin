package com.github.chenharryhua.nanjin

import akka.kafka.ConsumerMessage.CommittableMessage
import fs2.kafka.CommittableConsumerRecord

package object codec extends BitraverseAkkaMessage with BitraverseFs2Message {

  trait KafkaRecordCodec[K, V]
      extends KafkaConsumerRecordDecode[K, V] with KafkaProducerRecordEncode[K, V]

  abstract class Fs2Codec[F[_], K, V](keyCodec: KafkaCodec[K], valueCodec: KafkaCodec[V])
      extends KafkaMessageDecode[CommittableConsumerRecord[F, ?, ?], K, V](keyCodec, valueCodec)
      with Fs2MessageEncode[F, K, V] with KafkaConsumerRecordDecode[K, V]

  abstract class AkkaCodec[K, V](keyCodec: KafkaCodec[K], valueCodec: KafkaCodec[V])
      extends KafkaMessageDecode[CommittableMessage, K, V](keyCodec, valueCodec)
      with AkkaMessageEncode[K, V] with KafkaConsumerRecordDecode[K, V]

}
