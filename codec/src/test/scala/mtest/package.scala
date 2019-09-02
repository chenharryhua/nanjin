import java.util.Optional

import com.github.chenharryhua.nanjin.codec.{
  BitraverseAkkaMessage,
  BitraverseFs2Message,
  BitraverseKafkaRecord
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Arbitrary.{arbitrary, _}
import org.scalacheck.Gen
import org.scalacheck.Gen.containerOfN

import scala.compat.java8.OptionConverters._
import fs2.kafka.{
  CommittableConsumerRecord,
  Headers,
  ProducerRecords,
  Timestamp,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}
package object mtest extends BitraverseFs2Message with BitraverseAkkaMessage {

  val genHeaders: Gen[RecordHeaders] = for {
    key <- Gen.asciiPrintableStr
    value <- containerOfN[Array, Byte](2, arbitrary[Byte])
    rcs <- containerOfN[Array, Header](2, new RecordHeader(key, value): Header)
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

  val genFs2ConsumerRecord: Gen[Fs2ConsumerRecord[Int, Int]] =
    genConsumerRecord.map(fromKafkaConsumerRecord)

  val genFs2ProducerRecord: Gen[Fs2ProducerRecord[Int, Int]] =
    genProducerRecord.map(fromKafkaProducerRecord)
}
