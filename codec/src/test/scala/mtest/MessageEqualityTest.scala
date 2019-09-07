package mtest
import java.util.Optional

import cats.implicits._
import cats.kernel.laws.discipline.EqTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.{Header, Headers}
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class MessageEqualityTest extends AnyFunSuite with Discipline with KafkaRawMessageGen {

  implicit val arbOptionalInteger: Arbitrary[Optional[Integer]] = Arbitrary(genOptionalInteger)
  implicit val arbOptionalIntegerF: Arbitrary[Optional[Integer] => Optional[Integer]] =
    Arbitrary((x: Optional[Integer]) => x)

  implicit val arbitraryHeader: Arbitrary[Header] = Arbitrary(genHeader)
  implicit val arbitraryHeaderF: Arbitrary[Header => Header] =
    Arbitrary((a: Header) => a)
  implicit val arbitraryHeaders: Arbitrary[Headers] = Arbitrary(genHeaders)

  implicit val arbitraryHeadersF: Arbitrary[Headers => Headers] =
    Arbitrary((a: Headers) => a.add(new RecordHeader("a", Array(1, 2, 3): Array[Byte])))

  implicit val arbConsumerRecord: Arbitrary[ConsumerRecord[Int, Int]] = Arbitrary(genConsumerRecord)
  implicit val arbConsumerRecordF: Arbitrary[ConsumerRecord[Int, Int] => ConsumerRecord[Int, Int]] =
    Arbitrary((a: ConsumerRecord[Int, Int]) => a)

  implicit val arbProducerRecord: Arbitrary[ProducerRecord[Int, Int]] = Arbitrary(genProducerRecord)
  implicit val arbProducerRecordF: Arbitrary[ProducerRecord[Int, Int] => ProducerRecord[Int, Int]] =
    Arbitrary((a: ProducerRecord[Int, Int]) => a)

  checkAll("Array[Byte]", EqTests[Array[Byte]].eqv)
  checkAll("Header", EqTests[Header].eqv)
  checkAll("Headers", EqTests[Headers].eqv)
  checkAll("Optional[Integer]", EqTests[Optional[Integer]].eqv)
  checkAll("ConsumerRecord[Int,Int]", EqTests[ConsumerRecord[Int, Int]].eqv)
  checkAll("ProducerRecord[Int,Int]", EqTests[ProducerRecord[Int, Int]].eqv)
}
