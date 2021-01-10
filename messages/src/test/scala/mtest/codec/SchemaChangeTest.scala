package mtest.codec

import cats.data.Ior
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}
import io.circe.optics.JsonPath.root

object SchemaChangeTestData {
  final case class Nest(a: Int)
  final case class Nest2(b: String)
  final case class UnderTest(a: Int, b: Nest :+: Nest2 :+: CNil)

  val schema =
    """
{
  "type": "record",
  "name": "UnderTest",
  "namespace": "to.be.changed",
  "fields": [
    {
      "name": "a",
      "type": "int"
    },
    {
      "name": "b",
      "type": [
        {
          "type": "record",
          "name": "Nest",
          "fields": [
            {
              "name": "a",
              "type": "int"
            }
          ]
        },
        {
          "type": "record",
          "name": "Nest2",
          "fields": [
            {
              "name": "b",
              "type": "string"
            }
          ]
        }
      ]
    }
  ]
}
        """

  val oldSchema: Schema           = AvroCodec.toSchema(schema)
  val codec: AvroCodec[UnderTest] = AvroCodec[UnderTest](schema).right.get
}

class SchemaChangeTest extends AnyFunSuite {
  import SchemaChangeTestData._
  test("change namespace") {
    val newCodec: AvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")

    val data = UnderTest(1, Coproduct(Nest(1)))
    val en   = newCodec.avroEncoder.encode(data)
    val res  = newCodec.avroDecoder.decode(en)
    assert(res == data)
  }

  test("namespace different should throw exception") {
    val newCodec: AvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")
    val data                           = UnderTest(1, Coproduct(Nest(1)))
    val en                             = codec.avroEncoder.encode(data)
    assertThrows[Exception](newCodec.avroDecoder.decode(en))
  }
  test("empty namespace is not allowed") {
    assertThrows[Exception](codec.withNamespace(""))
  }
  test("space in namespace is not allowed") {
    assertThrows[Exception](codec.withNamespace("a.b. .c"))
  }
  test("hyphen in namespace is not allowed") {
    assertThrows[Exception](codec.withNamespace("a.b.-.c"))
  }

  test("child schema") {
    val schema = AvroCodec.toSchema("""{"type":"record","name":"Nest2","fields":[{"name":"b","type":"string"}]}""")
    val child  = codec.child[Nest2](root.fields.index(1).`type`.index(1))
    assert(child.schema == schema)
    val data = Nest2("abc")
    assert(child.idConversion(data) == data)
    assertThrows[Exception](codec.child[Nest2](root.fields.index(1).`type`.index(2)))
  }
}
