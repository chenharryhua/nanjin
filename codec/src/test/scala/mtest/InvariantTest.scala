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
  implicit val eqKafkaCodec: Eq[KafkaCodec[Int]] =
    (x: KafkaCodec[Int], y: KafkaCodec[Int]) => true

  checkAll("KafkaCodec", InvariantTests[KafkaCodec].invariant[Int, Int, Int])

  val tf: KafkaCodec[Int] = intCodec.imap(_ + 1)(_ - 1)
  check(forAll(arbitrary[Int]) { x =>
    tf.decode(tf.encode(x)) == x
  })
}
