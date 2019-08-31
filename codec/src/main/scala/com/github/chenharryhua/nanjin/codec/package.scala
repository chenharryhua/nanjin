package com.github.chenharryhua.nanjin

import akka.kafka.ConsumerMessage.CommittableMessage
import fs2.kafka.CommittableConsumerRecord
import monocle.Iso

package object codec extends BitraverseAkkaMessage with BitraverseFs2Message {

  trait KafkaRecordCodec[K, V]
      extends KafkaConsumerRecordDecode[K, V] with KafkaProducerRecordEncode[K, V]

  abstract class Fs2Codec[F[_], K, V](keyIso: Iso[Array[Byte], K], valueIso: Iso[Array[Byte], V])
      extends KafkaMessageDecode[CommittableConsumerRecord[F, ?, ?], K, V](keyIso, valueIso)
      with Fs2MessageEncode[F, K, V] with KafkaConsumerRecordDecode[K, V]

  abstract class AkkaCodec[K, V](keyIso: Iso[Array[Byte], K], valueIso: Iso[Array[Byte], V])
      extends KafkaMessageDecode[CommittableMessage, K, V](keyIso, valueIso)
      with AkkaMessageEncode[K, V] with KafkaConsumerRecordDecode[K, V]

}
