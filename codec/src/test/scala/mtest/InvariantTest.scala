package mtest

import cats.Eq
import cats.implicits._
import cats.laws.discipline.InvariantTests
import com.github.chenharryhua.nanjin.codec.KafkaCodec
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class InvariantTest extends AnyFunSuite with Discipline {

  implicit val arbKafkaCodec: Arbitrary[KafkaCodec[Int]] = Arbitrary(intCodec)
  implicit val eqKafkaCodec = new Eq[KafkaCodec[Int]] {
    override def eqv(x: KafkaCodec[Int], y: KafkaCodec[Int]): Boolean = true
  }

  checkAll("KafkaCodec", InvariantTests[KafkaCodec].invariant[Int, Int, Int])

  forAll(arbitrary[Int]) { x =>
    val tf = intCodec.imap(_ + 1)(_ - 1)
    tf.decode(tf.encode(x)) == x
  }
}
