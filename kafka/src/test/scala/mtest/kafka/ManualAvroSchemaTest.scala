package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema
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
  "namespace" : "mtest.kafka.ManualAvroSchemaTestData",
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
  "namespace" : "mtest.kafka.ManualAvroSchemaTestData",
  "fields": [
    {
      "name": "a",
      "type": "int",
      "doc" : "a type"
    },
    {
      "name": "b",
      "type": ["null","string"],
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

  test("should be semantically identical") {
    ManualAvroSchema.unsafeFrom[UnderTest](UnderTest.schema1).avroDecoder.schema
    intercept[IllegalArgumentException](
      ManualAvroSchema.unsafeFrom[UnderTest](UnderTest.schema2).avroDecoder.schema)
    intercept[IllegalArgumentException](
      ManualAvroSchema.unsafeFrom[UnderTest](UnderTest.schema3).avroDecoder.schema)
  }
/*
  test("should compile") {
    val schema: ManualAvroSchema[UnderTest] =
      ManualAvroSchema[UnderTest]("""
{
  "type": "record",
  "name": "UnderTest",
  "doc" : "test",
  "namespace" : "mtest.kafka.ManualAvroSchemaTestData",
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
        """)
  }
*/
}
