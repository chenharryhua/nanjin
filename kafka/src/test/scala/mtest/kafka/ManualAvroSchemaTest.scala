package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema
import org.scalatest.funsuite.AnyFunSuite

object ManualAvroSchemaTestData {

  final case class UnderTest(a: Int, b: String)

  object UnderTest {

    val schema =
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

  }

}

class ManualAvroSchemaTest extends AnyFunSuite {
  import ManualAvroSchemaTestData._
  test("considered to be the same schema when doc field added") {
    ManualAvroSchema[UnderTest](UnderTest.schema).schema
  }
}
