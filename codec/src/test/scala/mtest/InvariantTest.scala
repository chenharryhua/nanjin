package mtest

import cats.Eq
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.laws.discipline.InvariantTests
import com.github.chenharryhua.nanjin.codec.{KafkaCodec, SerdeOf}
import monocle.Prism
import org.scalacheck.Arbitrary

class InvariantTest extends AnyFunSuite with Discipline {
  val intPrism = SerdeOf[Int].asKey(sr).codec("topic")

  implicit val arbKafkaCodec: Arbitrary[KafkaCodec[Int]] = Arbitrary(intPrism)
  implicit val eqKafkaCodec = new Eq[KafkaCodec[Int]] {
    override def eqv(x: KafkaCodec[Int], y: KafkaCodec[Int]): Boolean = true
  }

  checkAll("KafkaCodec", InvariantTests[KafkaCodec].invariant[Int, Int, Int])

}
