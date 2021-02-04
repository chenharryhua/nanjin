package mtest.msg.codec

import cats.data.Ior
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
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
  import ManualAvroSchemaTestData._

  test("decoder/encoder have the same schema") {
    val input = (new Schema.Parser).parse(UnderTest.schema1)

    val ms1: Ior[String, AvroCodec[UnderTest]] =
      AvroCodec[UnderTest](UnderTest.schema1)

    assert(input == ms1.right.get.avroDecoder.schema)
    assert(input == ms1.right.get.avroEncoder.schema)
  }

  test("read-write incompatiable but acceptable") {
    val input = (new Schema.Parser).parse(UnderTest.schema2)

    val ms2: Ior[String, AvroCodec[UnderTest]] =
      AvroCodec[UnderTest](UnderTest.schema2)

    assert(input == ms2.right.get.avroDecoder.schema)
    assert(input == ms2.right.get.avroEncoder.schema)
    assert(ms2.isBoth)
  }

  test("incompatiable") {
    val input = (new Schema.Parser).parse(UnderTest.schema3)

    val ms3: Ior[String, AvroCodec[UnderTest]] =
      AvroCodec[UnderTest](UnderTest.schema3)

    assert(ms3.isLeft)
  }
}
