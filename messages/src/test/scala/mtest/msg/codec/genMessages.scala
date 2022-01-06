package mtest.msg.codec

import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{
  CommittableMessage as AkkaConsumerMessage,
  TransactionalMessage as AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message as AkkaProducerMessage, MultiMessage as AkkaMultiMessage}
import akka.kafka.testkit.ConsumerResultFactory
import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.Chunk
import fs2.kafka.{
  CommittableProducerRecords as Fs2CommittableProducerRecords,
  ConsumerRecord as Fs2ConsumerRecord,
  ProducerRecord as Fs2ProducerRecord,
  TransactionalProducerRecords as Fs2TransactionalProducerRecords
}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Arbitrary.{arbitrary, *}
import org.scalacheck.Gen

import java.util.Optional
import scala.compat.java8.OptionConverters.*

object genMessages {

  trait GenKafkaMessage { self =>

    val genHeader: Gen[Header] = for {
      key <- Gen.asciiPrintableStr
      value <-
        Gen.containerOfN[Array, Byte](2, arbitrary[Byte]) // avoid GC overhead limit exceeded issue
    } yield new RecordHeader(key, value)

    val genHeaders: Gen[RecordHeaders] = for {
      rcs <- Gen.containerOfN[Array, Header](2, genHeader) // avoid GC overhead limit exceeded issue
    } yield new RecordHeaders(rcs)

    val genOptionalInteger: Gen[Optional[Integer]] =
      Gen.option(arbitrary[Int].map(x => Integer.valueOf(x))).map(_.asJava)

    val genConsumerRecord: Gen[ConsumerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      timestampType <- Gen.oneOf(
        List(TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME, TimestampType.NO_TIMESTAMP_TYPE))
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

  trait GenFs2Message extends GenKafkaMessage { self =>
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

    val genFs2ProducerRecords: Gen[ProducerRecords[String, Int, Int]] =
      for {
        prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](2, genFs2ProducerRecord)
      } yield ProducerRecords(Chunk.seq(prs), "pass-through-fs2")

    val genFs2CommittableProducerRecords: Gen[Fs2CommittableProducerRecords[IO, Int, Int]] = for {
      pr <- genFs2ProducerRecord
      prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](10, pr).map(Chunk.seq)
      os <- genFs2CommittableOffset
    } yield Fs2CommittableProducerRecords(prs, os)

    val genFs2TransactionalProducerRecords: Gen[Fs2TransactionalProducerRecords[IO, String, Int, Int]] = for {
      cpr <- genFs2CommittableProducerRecords
      cprs <-
        Gen.containerOfN[List, Fs2CommittableProducerRecords[IO, Int, Int]](10, cpr).map(Chunk.seq)
    } yield Fs2TransactionalProducerRecords(cprs, "path-through")

  }

  trait GenAkkaMessage extends GenKafkaMessage { self =>
    import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset}

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
      ConsumerResultFactory.committableOffset(ConsumerMessage.PartitionOffset(gtp, offset), ""))

    val genAkkaProducerMessage: Gen[AkkaProducerMessage[Int, Int, String]] = for {
      cr <- genProducerRecord
    } yield AkkaProducerMessage(cr, "pass-through-akka")

    val genAkkaProducerMultiMessage: Gen[AkkaMultiMessage[Int, Int, String]] =
      Gen
        .containerOfN[List, ProducerRecord[Int, Int]](10, genProducerRecord)
        .map(rs => AkkaMultiMessage(rs, "pass-through-multi"))

    val genAkkaTransactionalMessage: Gen[AkkaTransactionalMessage[Int, Int]] = for {
      cr <- genConsumerRecord
      gtp <- genAkkaGroupTopicPartition
      offset <- Gen.posNum[Long]
    } yield AkkaTransactionalMessage(cr, new PartitionOffset(gtp, offset))
  }
}
