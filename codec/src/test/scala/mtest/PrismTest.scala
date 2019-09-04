package mtest

import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{BitraverseKafkaRecord, KJson, SerdeOf}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import monocle.Prism
import monocle.law.discipline.PrismTests
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import io.circe.generic.auto._
final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)

class PrismTest extends AnyFunSuite with Discipline with BitraverseKafkaRecord {

  val pc = for {
    a <- arbitrary[Int]
    b <- arbitrary[Long]
    c <- arbitrary[Float]
    d <- arbitrary[Double]
    e <- arbitrary[String]
  } yield PrimitiveTypeCombined(a, b, c, d, e)

  implicit val arbPrimitiveTypeCombined = Arbitrary(pc)

  implicit val eqPrimitiveTypeCombined = cats.derived.semi.eq[PrimitiveTypeCombined]
  implicit val arbClassF               = Arbitrary((a: PrimitiveTypeCombined) => a)

  val sr: Map[String, String] =
    Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")
  val strPrism       = SerdeOf[String].asValue(sr).codec("topic").prism
  val intPrism       = SerdeOf[Int].asKey(sr).codec("topic").prism
  val longPrism      = SerdeOf[Long].asValue(sr).codec("topic").prism
  val doublePrism    = SerdeOf[Double].asValue(sr).codec("topic").prism
  val floatPrism     = SerdeOf[Float].asKey(sr).codec("topic").prism
  val byteArrayPrism = SerdeOf[Array[Byte]].asKey(sr).codec("topic").prism

  val primitivies: Prism[Array[Byte], PrimitiveTypeCombined] =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec(s"prism.test.topic.avro").prism

  val json: Prism[Array[Byte], KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asKey(sr).codec(s"prism.test.topic.json").prism

  implicit val arbJson: Arbitrary[KJson[PrimitiveTypeCombined]] =
    Arbitrary(pc.map(KJson(_)))
  implicit val arbJsonF = Arbitrary((a: KJson[PrimitiveTypeCombined]) => a)

  implicit val arbStr: Arbitrary[String] = Arbitrary(Gen.asciiPrintableStr)
  implicit val arbArrayByte: Arbitrary[Array[Byte]] = Arbitrary(
    Gen.asciiPrintableStr.map(_.getBytes))

  checkAll("String", PrismTests(strPrism))
  checkAll("Int", PrismTests(intPrism))
  checkAll("Long", PrismTests(longPrism))
  checkAll("Double", PrismTests(doublePrism))
  checkAll("Float", PrismTests(floatPrism))
  checkAll("Primitivies", PrismTests(primitivies))
  checkAll("Json", PrismTests(json))
  checkAll("Array[Byte]", PrismTests(byteArrayPrism))
}
