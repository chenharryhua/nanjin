package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.isoGenericRecord
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import io.scalaland.chimney.Iso
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class Avro4sIsoSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  // ===== Test Model =====
  case class Foo(a: String, b: Int)

  given SchemaFor[Foo] = SchemaFor[Foo]
  given Encoder[Foo] = Encoder[Foo]
  given Decoder[Foo] = Decoder[Foo]

  // ===== Helper =====
  def roundTrip[A](iso: Iso[GenericRecord, A])(a: A): A =
    iso.first.transform(iso.second.transform(a))

  // ===== Generators =====
  given Arbitrary[Foo] = Arbitrary {
    for {
      a <- Gen.alphaStr
      b <- Gen.choose(Int.MinValue, Int.MaxValue)
    } yield Foo(a, b)
  }

  // ===== Tests =====

  test("round-trip A -> GenericRecord -> A") {
    val iso = isoGenericRecord[Foo]

    val original = Foo("hello", 42)
    val decoded = roundTrip(iso)(original)

    decoded shouldBe original
  }

  test("round-trip property holds for A") {
    val iso = isoGenericRecord[Foo]

    forAll { (foo: Foo) =>
      roundTrip(iso)(foo) shouldBe foo
    }
  }

  test("GenericRecord round-trip is NOT guaranteed (documenting behavior)") {
    val iso = isoGenericRecord[Foo]

    val schema = SchemaFor[Foo].schema
    val record = new GenericData.Record(schema)
    record.put("a", "hello")
    record.put("b", 42)

    val decoded = iso.first.transform(record)
    val encoded = iso.second.transform(decoded)

    // Not asserting equality — just ensuring it runs and documenting behavior
    encoded should not be null
  }
}
