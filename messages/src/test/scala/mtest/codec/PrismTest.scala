package mtest.codec

import cats.Eq
import com.github.chenharryhua.nanjin.messages.kafka._
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import monocle.law.discipline.PrismTests
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class PrismTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val pc: Gen[PrimitiveTypeCombined] = for {
    a <- arbitrary[Int]
    b <- arbitrary[Long]
    c <- arbitrary[Float]
    d <- arbitrary[Double]
    e <- arbitrary[String]
  } yield PrimitiveTypeCombined(a, b, c, d, e)

  implicit val arbPrimitiveTypeCombined: Arbitrary[PrimitiveTypeCombined] =
    Arbitrary(pc)

  implicit val eqPrimitiveTypeCombined: Eq[PrimitiveTypeCombined] =
    cats.derived.semiauto.eq[PrimitiveTypeCombined]

  implicit val arbClassF: Arbitrary[PrimitiveTypeCombined => PrimitiveTypeCombined] =
    Arbitrary((a: PrimitiveTypeCombined) => a)

  implicit val arbJson: Arbitrary[KJson[PrimitiveTypeCombined]] =
    Arbitrary(pc.map(KJson(_)))

  implicit val arbJsonF: Arbitrary[KJson[PrimitiveTypeCombined] => KJson[PrimitiveTypeCombined]] =
    Arbitrary((a: KJson[PrimitiveTypeCombined]) => a)

  implicit val arbStr: Arbitrary[String] = Arbitrary(Gen.asciiPrintableStr)

  implicit val arbArrayByte: Arbitrary[Array[Byte]] =
    Arbitrary(Gen.asciiPrintableStr.map(_.getBytes))

  checkAll("String", PrismTests(strCodec.prism))
  checkAll("Int", PrismTests(intCodec.prism))
  checkAll("Long", PrismTests(longCodec.prism))
  checkAll("Double", PrismTests(doubleCodec.prism))
  checkAll("Float", PrismTests(floatCodec.prism))
  checkAll("Primitivies", PrismTests(primitiviesCodec.prism))
  checkAll("Json", PrismTests(jsonPrimCodec.prism))
  checkAll("Array[Byte]", PrismTests(byteArrayCodec.prism))
}
