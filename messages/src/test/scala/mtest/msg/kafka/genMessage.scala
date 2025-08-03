package mtest.msg.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJHeader}
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
import io.scalaland.chimney.dsl.*

import java.util.Optional
import scala.jdk.OptionConverters.RichOption
object genMessage {

  trait GenKafkaMessage { self =>

    val genHeader: Gen[Header] = for {
      key <- Gen.asciiPrintableStr
      value <-
        Gen.containerOfN[Array, Byte](2, arbitrary[Byte]) // avoid GC overhead limit exceeded issue
    } yield new RecordHeader(key, value)

    val genNJHeader: Gen[NJHeader] = for {
      key <- Gen.asciiPrintableStr
      value <- Gen.containerOfN[Array, Byte](5, arbitrary[Byte])
    } yield NJHeader(key, value)

    val genHeaders: Gen[RecordHeaders] = for {
      rcs <- Gen.containerOfN[Array, Header](2, genHeader) // avoid GC overhead limit exceeded issue
    } yield new RecordHeaders(rcs)

    val genOptionalInteger: Gen[Optional[Integer]] =
      Gen.option(arbitrary[Int].map(x => Integer.valueOf(x))).map(_.toJava)

    val genConsumerRecord: Gen[ConsumerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      timestampType <- Gen.oneOf(
        List(TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME, TimestampType.NO_TIMESTAMP_TYPE))
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
      sizeKey,
      sizeValue,
      key,
      value,
      headers,
      lead)

    val genNJConsumerRecord: Gen[NJConsumerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      offset <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      timestampType <- Gen.oneOf(-1, 0, 1)
      key <- arbitrary[Int]
      value <- arbitrary[Int]
    } yield NJConsumerRecord(
      topic,
      partition,
      offset,
      timestamp,
      timestampType,
      Nil,
      None,
      None,
      None,
      Some(key),
      Some(value))

    val genProducerRecord: Gen[ProducerRecord[Int, Int]] = for {
      topic <- Gen.asciiPrintableStr
      partition <- Gen.posNum[Int]
      _ <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
      key <- arbitrary[Int]
      value <- arbitrary[Int]
      headers <- genHeaders
    } yield new ProducerRecord(topic, partition, timestamp, key, value, headers)
  }

  trait GenFs2Message extends GenKafkaMessage { self =>
    import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ProducerRecords}

    val genFs2ConsumerRecord: Gen[Fs2ConsumerRecord[Int, Int]] =
      genConsumerRecord.map(_.transformInto[Fs2ConsumerRecord[Int, Int]])

    val genFs2ProducerRecord: Gen[Fs2ProducerRecord[Int, Int]] =
      genProducerRecord.map(_.transformInto[Fs2ProducerRecord[Int, Int]])

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
        _ => IO.unit)

    val genFs2ConsumerMessage: Gen[CommittableConsumerRecord[IO, Int, Int]] =
      for {
        rec <- genFs2ConsumerRecord
        offset <- genFs2CommittableOffset
      } yield CommittableConsumerRecord[IO, Int, Int](rec, offset)

    val genFs2ProducerRecords: Gen[ProducerRecords[Int, Int]] =
      for {
        prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](2, genFs2ProducerRecord)
      } yield ProducerRecords(Chunk.from(prs))

    val genFs2CommittableProducerRecords: Gen[Fs2CommittableProducerRecords[IO, Int, Int]] = for {
      pr <- genFs2ProducerRecord
      prs <- Gen.containerOfN[List, Fs2ProducerRecord[Int, Int]](10, pr).map(Chunk.from)
      os <- genFs2CommittableOffset
    } yield Fs2CommittableProducerRecords(prs, os)

    val genFs2TransactionalProducerRecords: Gen[Fs2TransactionalProducerRecords[IO, Int, Int]] = for {
      cpr <- genFs2CommittableProducerRecords
      _ <-
        Gen.containerOfN[List, Fs2CommittableProducerRecords[IO, Int, Int]](10, cpr).map(Chunk.from)
    } yield Fs2TransactionalProducerRecords.one(cpr)

  }

}
