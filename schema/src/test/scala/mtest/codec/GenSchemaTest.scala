package mtest.codec

import com.github.chenharryhua.nanjin.schema.avro.NJAvroSchema.{NJField, NJInt, NJRecord}
import com.sksamuel.avro4s.SchemaFor
import org.scalatest.funsuite.AnyFunSuite

class GenSchemaTest extends AnyFunSuite {
  test("from schema") {
    val field = NJField(
      "test",
      None,
      NJInt(None),
      None,
      List()
    )
    val field2 = NJField(
      "test2",
      None,
      NJInt(None),
      None,
      List()
    )
    val record = NJRecord("r", "nj", None, List(), List(field, field2))
    println(record.schema)
    println(field.`type`.schema)
  }
}
