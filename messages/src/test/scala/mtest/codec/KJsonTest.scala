package mtest.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, SerdeOf}
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.{Arbitrary, Gen, Properties}
import io.circe.generic.auto._

object KJsonTestData {
  final case class Base(a: Int, b: Json)
  final case class CompositionType(c: Int, base: Base)
}

class KJsonTest extends Properties("kjson") {
  import KJsonTestData._
  import io.circe.generic.auto._
  val goodJson: SerdeOf[KJson[CompositionType]] = SerdeOf[KJson[CompositionType]]

  val genPerson: Gen[KJson[CompositionType]] = for {
    a <- Gen.posNum[Int]
    c <- Gen.posNum[Int]
  } yield KJson(CompositionType(a, Base(c, s"""{"a":$a,"b":"b"}""".asJson)))

  implicit val arbPerson: Arbitrary[KJson[CompositionType]] = Arbitrary(genPerson)

  property("encode/decode identity") = forAll { (ct: KJson[CompositionType]) =>
    val en = goodJson.avroCodec.avroEncoder.encode(ct)
    val de = goodJson.avroCodec.avroDecoder.decode(en)
    (ct == de) && (ct == goodJson.avroCodec.idConversion(ct))
  }
}
