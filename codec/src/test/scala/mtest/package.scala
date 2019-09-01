import java.util.Optional

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Arbitrary.{arbitrary, _}
import org.scalacheck.Gen
import org.scalacheck.Gen.containerOfN

import scala.compat.java8.OptionConverters._

package object mtest {

  val genHeaders: Gen[RecordHeaders] = for {
    key <- Gen.asciiPrintableStr
    value <- containerOfN[Array, Byte](8, arbitrary[Byte])
    rcs <- containerOfN[Array, Header](5, new RecordHeader(key, value): Header)
  } yield new RecordHeaders(rcs)

  val genOptionalInteger: Gen[Optional[Integer]] =
    Gen.option(arbitrary[Int].map(x => new Integer(x))).map(_.asJava)

  val genConsumerRecord = for {
    topic <- arbitrary[String]
    partition <- arbitrary[Int]
    offset <- arbitrary[Long]
    timestamp <- arbitrary[Long]
    checksum <- arbitrary[Long]
    s1 <- arbitrary[Int]
    s2 <- arbitrary[Int]
    key <- arbitrary[Int]
    value <- arbitrary[Int]
    headers <- genHeaders
    lead <- genOptionalInteger
  } yield new ConsumerRecord(
    topic,
    partition,
    offset,
    timestamp,
    TimestampType.NO_TIMESTAMP_TYPE,
    checksum,
    s1,
    s2,
    key,
    value,
    headers,
    lead)

  val genProducerRecord = for {
    topic <- arbitrary[String]
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    timestamp <- Gen.posNum[Long]
    key <- arbitrary[Int]
    value <- arbitrary[Int]
    headers <- genHeaders
  } yield new ProducerRecord(topic, partition, timestamp, key, value, headers)

}
