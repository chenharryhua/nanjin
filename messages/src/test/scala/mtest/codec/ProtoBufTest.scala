package mtest.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KPB, KafkaDeserializer, KafkaSerializer}
import mtest.pb.test.Person
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.{Arbitrary, Gen, Properties}
import scalapb.UnknownFieldSet

class ProtoBufTest extends Properties("protobuf") {

  val ser: KafkaSerializer[KPB[Person]]     = KafkaSerializer[KPB[Person]]
  val serDe: KafkaDeserializer[KPB[Person]] = KafkaDeserializer[KPB[Person]]

  val genPerson: Gen[Person] = for {
    name <- Gen.asciiStr
    age <- Gen.posNum[Int]
    unknows <- Gen.choose[Int](5, 10)
    along <- Gen.nonEmptyListOf[Long](Gen.posNum[Long])
  } yield Person(
    name,
    age,
    UnknownFieldSet.empty.withField(unknows, UnknownFieldSet.Field(fixed64 = along.toVector)))

  implicit val arbPerson: Arbitrary[Person] = Arbitrary(genPerson)

  property("encode/decode identity") = forAll { (p: Person) =>
    val encoded = ser.avroEncoder.encode(KPB(p))
    val decoded = serDe.avroDecoder.decode(encoded)
    decoded.value == p
  }
}
