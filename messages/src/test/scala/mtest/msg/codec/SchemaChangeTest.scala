package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}

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

  val oldSchema: Schema             = NJAvroCodec.toSchema(schema)
  val codec: NJAvroCodec[UnderTest] = NJAvroCodec[UnderTest](schema).right.get
}

class SchemaChangeTest extends AnyFunSuite {
  import SchemaChangeTestData.*
  test("change namespace") {
    val newCodec: NJAvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")

    val data = UnderTest(1, Coproduct(Nest(1)))
    val en   = newCodec.avroEncoder.encode(data)
    val res  = newCodec.avroDecoder.decode(en)
    assert(res == data)
  }

  test("namespace different should throw exception") {
    val newCodec: NJAvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")
    val data                             = UnderTest(1, Coproduct(Nest(1)))
    val en                               = codec.avroEncoder.encode(data)
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

  val schemaWithoutNamespace =
    """{"type":"record","name":"UnderTest","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","fields":[{"name":"b","type":"string"}]}]}]}"""

  val expected: Schema = NJAvroCodec.toSchema(schemaWithoutNamespace)

  test("remove namespace from schema") {
    assert(codec.withoutNamespace.schema == expected)
  }

  test("remove namespace from non-namespace schema") {
    val codec: NJAvroCodec[UnderTest] = NJAvroCodec[UnderTest](schemaWithoutNamespace).right.get
    assert(codec.withoutNamespace.schema == expected)
  }

  test("remove namespace is idempotent") {
    assert(codec.withoutNamespace.withoutNamespace.schema == codec.withoutNamespace.schema)
  }
}
