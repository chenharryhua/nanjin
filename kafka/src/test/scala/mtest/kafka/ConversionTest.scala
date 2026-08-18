package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.utils.*
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite

class ConversionTest extends AnyFunSuite {

  private val schema: Schema = SchemaBuilder
    .record("TestRecord")
    .namespace("mtest")
    .fields()
    .requiredString("name")
    .requiredInt("age")
    .endRecord()

  private def buildRecord(name: String, age: Int): GenericData.Record =
    new GenericRecordBuilder(schema).set("name", name).set("age", age).build()

  // --- genericRecord2Jackson ---

  test("1.genericRecord2Jackson - produces valid JSON string") {
    val record = buildRecord("Alice", 30)
    val result = genericRecord2Jackson(record)
    assert(result.isSuccess)
    val json = result.get
    assert(json.contains("\"name\""))
    assert(json.contains("\"Alice\""))
    assert(json.contains("\"age\""))
    assert(json.contains("30"))
  }

  // --- genericRecord2Circe ---

  test("2.genericRecord2Circe - produces valid circe Json") {
    val record = buildRecord("Bob", 25)
    val result = genericRecord2Circe(record)
    assert(result.isSuccess)
    val json = result.get
    assert(json.hcursor.get[String]("name").toOption.contains("Bob"))
    assert(json.hcursor.get[Int]("age").toOption.contains(25))
  }

  // --- genericRecord2BinAvro ---

  test("3.genericRecord2BinAvro - produces non-empty byte array") {
    val record = buildRecord("Charlie", 40)
    val result = genericRecord2BinAvro(record)
    assert(result.isSuccess)
    assert(result.get.nonEmpty)
  }

  // --- jackson2GenericRecord ---

  test("4.jackson2GenericRecord - round-trips with genericRecord2Jackson") {
    val original = buildRecord("Diana", 35)
    val jackson = genericRecord2Jackson(original).get
    val result = jackson2GenericRecord(schema, jackson)
    assert(result.isSuccess)
    assert(result.get.get("name").toString == "Diana")
    assert(result.get.get("age") == 35)
  }

  test("5.jackson2GenericRecord - invalid JSON fails") {
    val result = jackson2GenericRecord(schema, "not valid json")
    assert(result.isFailure)
  }

  test("6.jackson2GenericRecord - schema mismatch fails") {
    // JSON has wrong field types
    val result = jackson2GenericRecord(schema, """{"name": 123, "age": "not a number"}""")
    assert(result.isFailure)
  }

  // --- jsonNode2GenericRecord ---

  test("7.jsonNode2GenericRecord - from Jackson ObjectMapper") {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val node = mapper.readTree("""{"name": "Eve", "age": 28}""")
    val result = jsonNode2GenericRecord(node, schema)
    assert(result.isSuccess)
    assert(result.get.get("name").toString == "Eve")
    assert(result.get.get("age") == 28)
  }

  test("8.jsonNode2GenericRecord - invalid node fails") {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val node = mapper.readTree("""{"wrong": "fields"}""")
    val result = jsonNode2GenericRecord(node, schema)
    assert(result.isFailure)
  }

  // --- immigrate ---

  test("9.immigrate - reshapes record to target schema") {
    // Target schema adds a new field with default
    val targetSchema = SchemaBuilder
      .record("TestRecord")
      .namespace("mtest")
      .fields()
      .requiredString("name")
      .requiredInt("age")
      .name("city").`type`().stringType().stringDefault("unknown")
      .endRecord()

    val original = buildRecord("Frank", 50)
    val result = immigrate(targetSchema, original)
    assert(result.isSuccess)
    val migrated = result.get
    assert(migrated.get("name").toString == "Frank")
    assert(migrated.get("age") == 50)
    assert(migrated.get("city").toString == "unknown")
    assert(migrated.getSchema == targetSchema)
  }

  test("10.immigrate - null record returns null") {
    val result = immigrate(schema, null)
    assert(result.isSuccess)
    assert(result.get == null)
  }

  test("11.immigrate - incompatible schema fails") {
    // Schema with required field that the source doesn't have and has no default
    val incompatible = SchemaBuilder
      .record("Other")
      .namespace("mtest")
      .fields()
      .requiredString("name")
      .requiredInt("age")
      .requiredString("required_missing")
      .endRecord()

    val original = buildRecord("Grace", 22)
    val result = immigrate(incompatible, original)
    assert(result.isFailure)
  }

  // --- bin avro round-trip ---

  test("12.binary avro round-trip via genericRecord2BinAvro and immigrate") {
    val original = buildRecord("Hank", 45)
    val bytes = genericRecord2BinAvro(original).get

    // Manually decode from binary
    val bais = new java.io.ByteArrayInputStream(bytes)
    val decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(bais, null)
    val reader = new org.apache.avro.generic.GenericDatumReader[GenericData.Record](schema)
    val decoded = reader.read(null, decoder)

    assert(decoded.get("name").toString == "Hank")
    assert(decoded.get("age") == 45)
  }
}
