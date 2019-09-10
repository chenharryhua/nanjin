package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.codec.MessagePropertiesFs2
import fs2.Chunk
import org.scalacheck.Gen
import fs2.kafka.{
  CommittableConsumerRecord,
  CommittableOffset,
  ProducerRecords,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

trait Fs2MessageGen extends KafkaRawMessageGen with MessagePropertiesFs2 {

  val genFs2ConsumerRecord: Gen[Fs2ConsumerRecord[Int, Int]] =
    genConsumerRecord.map(fromKafkaConsumerRecord)

  val genFs2ProducerRecord: Gen[Fs2ProducerRecord[Int, Int]] =
    genProducerRecord.map(fromKafkaProducerRecord)

  val genCommittableOffset: Gen[CommittableOffset[IO]] =
    for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      meta <- Gen.asciiStr
      consumerGroupId <- Gen.option(Gen.asciiPrintableStr)
    } yield CommittableOffset(
      new TopicPartition(topic, partition),
      new OffsetAndMetadata(offset, meta),
      consumerGroupId,
      x => IO.unit)

  val genFs2ConsumerMessage: Gen[CommittableConsumerRecord[IO, Int, Int]] =
    for {
      rec <- genFs2ConsumerRecord
      offset <- genCommittableOffset
    } yield CommittableConsumerRecord[IO, Int, Int](rec, offset)

  val genFs2ProducerMessage: Gen[ProducerRecords[Int, Int, String]] =
    for {
      prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](2, genFs2ProducerRecord)
    } yield ProducerRecords(Chunk.seq(prs), "pass-through-fs2")

}
