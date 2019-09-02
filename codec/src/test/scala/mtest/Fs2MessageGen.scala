package mtest

import com.github.chenharryhua.nanjin.codec.BitraverseFs2Message
import org.scalacheck.Gen
import fs2.kafka.{ConsumerRecord => Fs2ConsumerRecord, ProducerRecord => Fs2ProducerRecord}

trait Fs2MessageGen extends KafkaRawMessageGen with BitraverseFs2Message {

  val genFs2ConsumerRecord: Gen[Fs2ConsumerRecord[Int, Int]] =
    genConsumerRecord.map(fromKafkaConsumerRecord)

  val genFs2ProducerRecord: Gen[Fs2ProducerRecord[Int, Int]] =
    genProducerRecord.map(fromKafkaProducerRecord)
}
