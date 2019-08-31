package mtest

import cats.Eq
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.SerdeOf
import monocle.law.discipline.PrismTests
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class IsoTest extends AnyFunSuite with Discipline {

  implicit val eqArrayByte: Eq[Array[Byte]] = (x: Array[Byte], y: Array[Byte]) =>
    x.zip(y).forall { case (x, y) => x.eqv(y) }

  val prism = SerdeOf[Int].asKey(Map()).prism("topic")

  checkAll("prism-key", PrismTests(prism.keyPrism))
  checkAll("prism-value", PrismTests(prism.valuePrism))
}
