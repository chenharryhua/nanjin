package mtest
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaConsumerMessage}
import akka.kafka.ProducerMessage.{Message            => AkkaProducerMessage}
import cats.effect.IO
import com.github.chenharryhua.nanjin.codec._
import fs2.Chunk
import fs2.kafka.{ConsumerRecord => Fs2ConsumerRecord, ProducerRecord => Fs2ProducerRecord}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Arbitrary.{arbitrary, _}
import org.scalacheck.Gen

import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

object genMessage {

  trait GenKafkaMessage {

    val genHeader: Gen[Header] = for {
      key <- Gen.asciiPrintableStr
      value <- Gen
        .containerOfN[Array, Byte](2, arbitrary[Byte]) //avoid GC overhead limit exceeded issue
    } yield new RecordHeader(key, value)

    val genHeaders: Gen[RecordHeaders] = for {
      rcs <- Gen.containerOfN[Array, Header](2, genHeader) //avoid GC overhead limit exceeded issue
    } yield new RecordHeaders(rcs)

    val genOptionalInteger: Gen[Optional[Integer]] =
      Gen.option(arbitrary[Int].map(x => new Integer(x))).map(_.asJava)

    val genConsumerRecord: Gen[ConsumerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      timestampType <- Gen.oneOf(List(TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME))
      checksum <- arbitrary[Long]
      sizeKey <- arbitrary[Int]
      sizeValue <- arbitrary[Int]
      key <- arbitrary[Int]
      value <- arbitrary[Int]
      headers <- genHeaders
      lead <- genOptionalInteger
    } yield new ConsumerRecord(
      topic,
      partition,
      offset,
      timestamp,
      timestampType,
      checksum,
      sizeKey,
      sizeValue,
      key,
      value,
      headers,
      lead)

    val genProducerRecord: Gen[ProducerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      key <- arbitrary[Int]
      value <- arbitrary[Int]
      headers <- genHeaders
    } yield new ProducerRecord(topic, partition, timestamp, key, value, headers)
  }

  trait GenFs2Message extends GenKafkaMessage {
    import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ProducerRecords}

    val genFs2ConsumerRecord: Gen[Fs2ConsumerRecord[Int, Int]] =
      genConsumerRecord.map(isoFs2ComsumerRecord.reverseGet)

    val genFs2ProducerRecord: Gen[Fs2ProducerRecord[Int, Int]] =
      genProducerRecord.map(isoFs2ProducerRecord.reverseGet)

    val genFs2CommittableOffset: Gen[CommittableOffset[IO]] =
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
        offset <- genFs2CommittableOffset
      } yield CommittableConsumerRecord[IO, Int, Int](rec, offset)

    val genFs2ProducerMessage: Gen[ProducerRecords[Int, Int, String]] =
      for {
        prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](2, genFs2ProducerRecord)
      } yield ProducerRecords(Chunk.seq(prs), "pass-through-fs2")

  }

  trait GenAkkaMessage extends GenKafkaMessage {
    import akka.kafka.ConsumerMessage.{CommittableOffset, GroupTopicPartition, PartitionOffset}

    val genAkkaGroupTopicPartition: Gen[GroupTopicPartition] = for {
      groupId <- Gen.asciiPrintableStr
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
    } yield GroupTopicPartition(groupId, topic, partition)

    val genAkkaConsumerMessage: Gen[AkkaConsumerMessage[Int, Int]] = for {
      cr <- genConsumerRecord
      gtp <- genAkkaGroupTopicPartition
      offset <- Gen.posNum[Long]
    } yield AkkaConsumerMessage(
      cr,
      new CommittableOffset {
        val partitionOffset: PartitionOffset                = new PartitionOffset(gtp, offset)
        override def commitScaladsl(): Future[Done]         = null
        override def commitJavadsl(): CompletionStage[Done] = null
        override def batchSize: Long                        = 0
      }
    )

    val genAkkaProducerMessage: Gen[AkkaProducerMessage[Int, Int, String]] = for {
      cr <- genProducerRecord
    } yield AkkaProducerMessage(cr, "pass-through-akka")
  }

}
