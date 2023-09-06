package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}
import eu.timepit.refined.auto.*

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

  val oldSchema: Schema             = (new Schema.Parser).parse(schema)
  val codec: NJAvroCodec[UnderTest] = NJAvroCodec[UnderTest](schema)
}

class SchemaChangeTest extends AnyFunSuite {
  import SchemaChangeTestData.*
  test("change namespace") {
    val newCodec: NJAvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")

    val data = UnderTest(1, Coproduct(Nest(1)))
    val en   = newCodec.encode(data)
    val res  = newCodec.decode(en)
    assert(res == data)
  }

  test("namespace different should throw exception") {
    val newCodec: NJAvroCodec[UnderTest] = codec.withNamespace("mtest.avro.SchemaChangeTestData")
    val data                             = UnderTest(1, Coproduct(Nest(1)))
    val en                               = codec.encode(data)
    assertThrows[Exception](newCodec.decode(en))
  }

  val schemaWithoutNamespace =
    """{"type":"record","name":"UnderTest","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","fields":[{"name":"b","type":"string"}]}]}]}"""

  val expected: Schema = (new Schema.Parser).parse(schemaWithoutNamespace)

  test("remove namespace from schema") {
    assert(codec.withoutNamespace.schema == expected)
  }

  test("remove namespace from non-namespace schema") {
    val codec: NJAvroCodec[UnderTest] = NJAvroCodec[UnderTest](schemaWithoutNamespace)
    assert(codec.withoutNamespace.schema == expected)
  }

  test("remove namespace is idempotent") {
    assert(codec.withoutNamespace.withoutNamespace.schema == codec.withoutNamespace.schema)
  }
}
