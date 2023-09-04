package mtest.msg.codec

import cats.data.Ior
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

object ManualAvroSchemaTestData {

  final case class UnderTest(a: Int, b: String)

  object UnderTest {

    val schema1 =
      """
{
  "type": "record",
  "name": "UnderTest",
  "doc" : "test",
  "namespace" : "mtest.avro.ManualAvroSchemaTestData",
  "fields": [
    {
      "name": "a",
      "type": "int",
      "doc" : "a type"
    },
    {
      "name": "b",
      "type": "string",
      "doc" : "b type"
    }
  ]
}        
        """

    val schema2 =
      """
{
  "type": "record",
  "name": "UnderTest",
  "doc" : "test",
  "namespace" : "mtest.avro.ManualAvroSchemaTestData",
  "fields": [
    {
      "name": "a",
      "type": ["null","int"],
      "doc" : "a type"
    },
    {
      "name": "b",
      "type": "string",
      "doc" : "b type"
    }
  ]
}        
        """

    val schema3 =
      """
{
  "type": "record",
  "name": "UnderTest",
  "doc" : "test",
  "namespace" : "namespace.diff",
  "fields": [
    {
      "name": "a",
      "type": "int",
      "doc" : "a type"
    },
    {
      "name": "b",
      "type": "string",
      "doc" : "b type"
    },
    {
      "name": "c",
      "type": ["null", "string"],
      "doc" : "b type"
    }
  ]
}        
        """
  }

}

class ManualAvroSchemaTest extends AnyFunSuite {
  import ManualAvroSchemaTestData.*

  test("decoder/encoder have the same schema") {
    val input = (new Schema.Parser).parse(UnderTest.schema1)

    val ms1: Ior[String, NJAvroCodec[UnderTest]] =
      NJAvroCodec[UnderTest](UnderTest.schema1)

    assert(input == ms1.right.get.schema)
    assert(input == ms1.right.get.schema)
  }

  test("read-write incompatiable but acceptable") {
    val input = (new Schema.Parser).parse(UnderTest.schema2)

    val ms2: Ior[String, NJAvroCodec[UnderTest]] =
      NJAvroCodec[UnderTest](UnderTest.schema2)

    assert(input == ms2.right.get.schema)
    assert(input == ms2.right.get.schema)
    assert(ms2.isBoth)
  }

  test("incompatiable") {
    (new Schema.Parser).parse(UnderTest.schema3)

    val ms3: Ior[String, NJAvroCodec[UnderTest]] =
      NJAvroCodec[UnderTest](UnderTest.schema3)

    assert(ms3.isLeft)
  }
}
