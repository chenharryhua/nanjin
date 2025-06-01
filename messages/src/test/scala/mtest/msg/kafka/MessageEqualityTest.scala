package mtest.msg.kafka

import cats.kernel.laws.discipline.EqTests
import com.github.chenharryhua.nanjin.messages.kafka.NJHeader
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.{Header, Headers}
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import java.util.Optional
class MessageEqualityTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitraryData.*

  implicit val arbOptionalInteger: Arbitrary[Optional[Integer]] = Arbitrary(genOptionalInteger)

  implicit val arbOptionalIntegerF: Arbitrary[Optional[Integer] => Optional[Integer]] =
    Arbitrary((x: Optional[Integer]) => x)

  implicit val arbitraryHeader: Arbitrary[Header] = Arbitrary(genHeader)

  implicit val arbitraryNJHeader: Arbitrary[NJHeader]              = Arbitrary(genNJHeader)
  implicit val arbitraryNJHeaderF: Arbitrary[NJHeader => NJHeader] =
    Arbitrary((a: NJHeader) => a)

  implicit val arbitraryHeaderF: Arbitrary[Header => Header] =
    Arbitrary((a: Header) => a)
  implicit val arbitraryHeaders: Arbitrary[Headers] = Arbitrary(genHeaders)

  implicit val arbitraryHeadersF: Arbitrary[Headers => Headers] =
    Arbitrary((a: Headers) => a.add(new RecordHeader("a", Array(1, 2, 3): Array[Byte])))

  checkAll("Header", EqTests[Header].eqv)
  checkAll("Headers", EqTests[Headers].eqv)
  checkAll("Optional[Integer]", EqTests[Optional[Integer]].eqv)
  checkAll("ConsumerRecord[Int,Int]", EqTests[ConsumerRecord[Int, Int]].eqv)
  checkAll("ProducerRecord[Int,Int]", EqTests[ProducerRecord[Int, Int]].eqv)
  checkAll("NJHeader", EqTests[NJHeader].eqv)
}
