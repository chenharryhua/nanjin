package mtest.kafka

import cats.kernel.laws.discipline.OrderTests
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.kafka.KafkaOffset
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.derived.auto.eq._

class KafkaOffsetTest extends CatsSuite with FunSuiteDiscipline {

  implicit val arb: Arbitrary[KafkaOffset] = Arbitrary(
    Gen.choose[Long](0, Long.MaxValue).map(KafkaOffset(_)))
 
  implicit val cogen: Cogen[KafkaOffset] =
    Cogen[KafkaOffset]((o: KafkaOffset) => o.value)

  checkAll("kafka offset", OrderTests[KafkaOffset].order)
}
