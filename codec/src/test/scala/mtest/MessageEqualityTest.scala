package mtest
import java.util.Optional

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.implicits._
import cats.Eq
import cats.kernel.laws.discipline.EqTests
import com.github.chenharryhua.nanjin.codec.BitraverseKafkaRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen.{choose, containerOf, containerOfN, nonEmptyContainerOf, oneOf}

import scala.compat.java8.OptionConverters._

class MessageEqualityTest extends AnyFunSuite with Discipline with BitraverseKafkaRecord {

  val genHeaders: Gen[RecordHeaders] = for {
    key <- Gen.asciiPrintableStr
    value <- containerOfN[Array, Byte](8, arbitrary[Byte])
    rcs <- containerOfN[Array, Header](5, new RecordHeader(key, value): Header)
  } yield new RecordHeaders(rcs)

  val genOptionalInteger: Gen[Optional[Integer]] =
    Gen.option(arbitrary[Int].map(x => new Integer(x))).map(_.asJava)

  implicit val arbOptionalInteger: Arbitrary[Optional[Integer]] = Arbitrary(genOptionalInteger)
  implicit val arbOptionalIntegerF: Arbitrary[Optional[Integer] => Optional[Integer]] =
    Arbitrary((x: Optional[Integer]) => x)

  implicit val arbitraryHeaders: Arbitrary[Headers] = Arbitrary(genHeaders)

  implicit val arbitraryHeadersF: Arbitrary[Headers => Headers] =
    Arbitrary((a: Headers) => a.add(new RecordHeader("a", Array(1, 2, 3): Array[Byte])))

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
  implicit val arbConsumerRecord = Arbitrary(genConsumerRecord)
  implicit val arbConsumerRecordF: Arbitrary[ConsumerRecord[Int, Int] => ConsumerRecord[Int, Int]] =
    Arbitrary((a: ConsumerRecord[Int, Int]) => a)

  val genProducerRecord = for {
    topic <- arbitrary[String]
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    timestamp <- Gen.posNum[Long]
    key <- arbitrary[Int]
    value <- arbitrary[Int]
    headers <- genHeaders
  } yield new ProducerRecord(topic, partition, timestamp, key, value, headers)
  implicit val arbProducerRecord  = Arbitrary(genProducerRecord)
  implicit val arbProducerRecordF = Arbitrary((a: ProducerRecord[Int, Int]) => a)

  checkAll("Array[Byte]", EqTests[Array[Byte]].eqv)
  checkAll("Headers", EqTests[Headers].eqv)
  checkAll("Optional[Integer]", EqTests[Optional[Integer]].eqv)
  checkAll("ConsumerRecord[Int,Int]", EqTests[ConsumerRecord[Int, Int]].eqv)
  checkAll("ProducerRecord[Int,Int]", EqTests[ProducerRecord[Int, Int]].eqv)
}
