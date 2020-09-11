package mtest.codec

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.funsuite.AnyFunSuite

class PrimitiveTypesTest extends AnyFunSuite {
  test("from schema") {
    val date =
      """
        |{
        |  "type": "int",
        |  "logicalType": "date"
        |}
        |""".stripMargin

  }
}
