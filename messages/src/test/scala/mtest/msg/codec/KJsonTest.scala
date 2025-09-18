package mtest.msg.codec

import cats.Id
import cats.derived.auto.eq.*
import cats.kernel.Eq
import cats.kernel.laws.discipline.EqTests
import cats.laws.discipline.DistributiveTests
import cats.syntax.all.*
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, KJson}
import io.circe.Json
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.{Arbitrary, Cogen, Gen, Properties}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

object KJsonTestData {
  final case class Base(a: Long, b: Json)
  final case class CompositionType(c: Int, base: Base)

  val goodJson: AvroFor[KJson[CompositionType]] = AvroFor[KJson[CompositionType]]

  val genCT: Gen[CompositionType] = for {
    a <- Gen.posNum[Long]
    c <- Gen.posNum[Int]
  } yield CompositionType(c, Base(a, s"""{"a":$a,"b":"b"}""".asJson))

  implicit val arbiCT: Arbitrary[CompositionType] = Arbitrary(genCT)

  val genKJson: Gen[KJson[CompositionType]] = genCT.map(KJson(_))

  implicit val arbKJson: Arbitrary[KJson[CompositionType]] = Arbitrary(genKJson)

  implicit val cogen: Cogen[KJson[CompositionType]] =
    Cogen[KJson[CompositionType]]((a: KJson[CompositionType]) => a.value.base.a)

}

class KJsonTest extends Properties("kjson") {
  import KJsonTestData.*

  val genKJsons: Gen[List[KJson[CompositionType]]] = Gen.listOfN(20, genKJson)
  implicit val arbKJsons: Arbitrary[List[KJson[CompositionType]]] = Arbitrary(genKJsons)

  property("encode/decode identity") = forAll { (ct: KJson[CompositionType]) =>
    val en = goodJson.avroCodec.encode(ct)
    val de = goodJson.avroCodec.decode(en)
    ct == de && ct === goodJson.avroCodec.idConversion(ct)
  }

  property("encode/decode collection identity") = forAll { (ct: List[KJson[CompositionType]]) =>
    val id = ct.map(goodJson.avroCodec.idConversion)
    ct == id && ct === id
  }
}

class KJsonEqTest extends CatsSuite with FunSuiteDiscipline {
  import KJsonTestData.*

  implicit val cogenCT: Cogen[CompositionType] =
    Cogen[CompositionType]((a: CompositionType) => a.base.a)

  implicit val infereq0: Eq[CompositionType] = cats.derived.semiauto.eq[CompositionType]

  checkAll("KJson", EqTests[KJson[CompositionType]].eqv)
  checkAll(
    "KJson",
    DistributiveTests[KJson].distributive[CompositionType, CompositionType, CompositionType, List, Id])
}
