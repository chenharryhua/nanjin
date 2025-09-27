package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

object ManualAvroSchemaTestData {

  @JsonCodec
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

    val schema4 =
      """
{
          "type": "record",
          "name": "UnderTest",
          "namespace": "only.namespace.diff",
          "fields":
          [
              {
                  "name": "a",
                  "type": "int"
              },
              {
                  "name": "b",
                  "type": "string"
              }
          ]
      }
      """

    val schema5 =
      """
{
          "type": "record",
          "name": "UnderTest",
          "namespace": "mtest.msg.codec.ManualAvroSchemaTestData",
          "fields":
          [
              {
                  "name": "a",
                  "type": "int"
              },
              {
                  "name": "b",
                  "type": "string"
              },
              {
                  "name": "c",
                  "type":
                  [
                      "null",
                      "string"
                  ],
                  "default": null
              }
          ]
      }
         """

    val schema6 =
      """
         {
          "type": "record",
          "name": "UnderTest",
          "namespace": "mtest.msg.codec.ManualAvroSchemaTestData",
          "fields":
          [
              {
                  "name": "a",
                  "type": "int"
              }
          ]
      }
         """
  }
}

class ManualAvroSchemaTest extends AnyFunSuite {
  import ManualAvroSchemaTestData.*

  test("add doc field") {
    AvroCodec[UnderTest](UnderTest.schema1)
  }

  test("became optional a") {
    assertThrows[Exception](AvroCodec[UnderTest](UnderTest.schema2))
  }

  test("add optional c without default") {
    assertThrows[Exception](AvroCodec[UnderTest](UnderTest.schema3))
  }

  test("add optional c with default") {
    assertThrows[Exception](AvroCodec[UnderTest](UnderTest.schema5))
  }

  test("only namespace is different") {
    AvroCodec[UnderTest](UnderTest.schema4)
  }
  test("remove b") {
    assertThrows[Exception](AvroCodec[UnderTest](UnderTest.schema6))
  }
}
