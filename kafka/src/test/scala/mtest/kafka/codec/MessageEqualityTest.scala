package mtest.kafka.codec

import java.util.Optional

import cats.implicits._
import cats.kernel.laws.discipline.EqTests
import com.github.chenharryhua.nanjin.codec.eq._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.{Header, Headers}
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class MessageEqualityTest extends AnyFunSuite with Discipline {

  implicit val arbOptionalInteger: Arbitrary[Optional[Integer]] = Arbitrary(genOptionalInteger)

  implicit val arbOptionalIntegerF: Arbitrary[Optional[Integer] => Optional[Integer]] =
    Arbitrary((x: Optional[Integer]) => x)

  implicit val arbitraryHeader: Arbitrary[Header] = Arbitrary(genHeader)

  implicit val arbitraryHeaderF: Arbitrary[Header => Header] =
    Arbitrary((a: Header) => a)
  implicit val arbitraryHeaders: Arbitrary[Headers] = Arbitrary(genHeaders)

  implicit val arbitraryHeadersF: Arbitrary[Headers => Headers] =
    Arbitrary((a: Headers) => a.add(new RecordHeader("a", Array(1, 2, 3): Array[Byte])))

  checkAll("Array[Byte]", EqTests[Array[Byte]].eqv)
  checkAll("Header", EqTests[Header].eqv)
  checkAll("Headers", EqTests[Headers].eqv)
  checkAll("Optional[Integer]", EqTests[Optional[Integer]].eqv)
  checkAll("ConsumerRecord[Int,Int]", EqTests[ConsumerRecord[Int, Int]].eqv)
  checkAll("ProducerRecord[Int,Int]", EqTests[ProducerRecord[Int, Int]].eqv)
}
