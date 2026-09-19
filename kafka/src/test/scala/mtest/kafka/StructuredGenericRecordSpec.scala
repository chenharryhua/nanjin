package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.{Structured, Unregistered}
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

import java.util
import java.util.Optional

class StructuredGenericRecordSpec extends FunSuite {

  class FakeSchemaRegistryClient extends io.confluent.kafka.schemaregistry.client.SchemaRegistryClient {
    override def parseSchema(
      schemaType: String,
      schemaString: String,
      references: util.List[SchemaReference]): Optional[ParsedSchema] = ???
    override def register(subject: String, schema: ParsedSchema): Int = ???
    override def register(subject: String, schema: ParsedSchema, version: Int, id: Int): Int = ???
    override def getSchemaById(id: Int): ParsedSchema = ???
    override def getSchemaBySubjectAndId(subject: String, id: Int): ParsedSchema = ???
    override def getAllSubjectsById(id: Int): util.Collection[String] = ???
    override def getLatestSchemaMetadata(subject: String): SchemaMetadata = ???
    override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = ???
    override def getVersion(subject: String, schema: ParsedSchema): Int = ???
    override def getAllVersions(subject: String): util.List[Integer] = ???
    override def testCompatibility(subject: String, schema: ParsedSchema): Boolean = ???
    override def setMode(mode: String): String = ???
    override def setMode(mode: String, subject: String): String = ???
    override def getMode: String = ???
    override def getMode(subject: String): String = ???
    override def getAllSubjects: util.Collection[String] = ???
    override def getId(subject: String, schema: ParsedSchema): Int = ???
    override def reset(): Unit = ???
  }

  val schema: Schema =
    new Schema.Parser().parse(
      """
        |{
        | "type": "record",
        | "name": "Test",
        | "fields": [
        |   {"name": "id", "type": "int"},
        |   {"name": "name", "type": "string"}
        | ]
        |}
        |""".stripMargin
    )

  def record(id: Int, name: String): GenericRecord = {
    val r = new GenericData.Record(schema)
    r.put("id", id)
    r.put("name", name)
    r
  }

  val client = new FakeSchemaRegistryClient
  val structured: Unregistered[GenericRecord] = summon[Structured[GenericRecord]]
  val serde: Serde[GenericRecord] = ctx.asKey(structured).serde

  test("round-trip GenericRecord") {
    val bytes = serde.serializer.serialize("topic", record(1, "a"))
    val out = serde.deserializer.deserialize("topic", bytes)

    assertEquals(out.get("id").asInstanceOf[Int], 1)
    assertEquals(out.get("name").toString, "a")
  }

  test("support headers") {
    val headers = new RecordHeaders().add("k", "v".getBytes)

    val bytes = serde.serializer.serialize("topic", headers, record(2, "b"))
    val out = serde.deserializer.deserialize("topic", bytes)

    assertEquals(out.get("id").asInstanceOf[Int], 2)
  }

  test("handle null") {
    val bytes = serde.serializer.serialize("topic", null)
    assertEquals(serde.deserializer.deserialize("topic", bytes), null)
  }

  test("fail on corrupted input") {
    intercept[Exception] {
      serde.deserializer.deserialize("topic", Array(1, 2, 3))
    }
  }
}
