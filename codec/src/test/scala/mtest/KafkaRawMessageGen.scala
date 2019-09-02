package mtest

import java.util.Optional

import com.github.chenharryhua.nanjin.codec.BitraverseKafkaRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Arbitrary.{arbitrary, _}
import org.scalacheck.Gen

import scala.compat.java8.OptionConverters._

trait KafkaRawMessageGen extends BitraverseKafkaRecord {

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
