package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.schema.*
import org.apache.avro.{Schema, SchemaFormatter}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaOpsTest extends AnyFunSuite with Matchers {

  private def pretty(schema: Schema): String =
    SchemaFormatter.format("json/pretty", schema)

  private def compact(schema: Schema): String =
    SchemaFormatter.format("json", schema)

  private def parseable(schema: Schema): Assertion =
    noException should be thrownBy
      new Schema.Parser().parse(compact(schema))

  private val schemaJson =
    """
      |{
      |  "type": "record",
      |  "name": "Person",
      |  "namespace": "old.ns",
      |  "doc": "top level doc",
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string",
      |      "default": ""
      |    },
      |    {
      |      "name": "address",
      |      "type": {
      |        "type": "record",
      |        "name": "Address",
      |        "namespace": "child.ns",
      |        "doc": "address doc",
      |        "fields": [
      |          {
      |            "name": "street",
      |            "type": "string",
      |            "default": "unknown"
      |          }
      |        ]
      |      }
      |    },
      |    {
      |      "name": "tags",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "Tag",
      |          "namespace": "tag.ns",
      |          "doc": "tag doc",
      |          "fields": [
      |            {
      |              "name": "value",
      |              "type": "string",
      |              "default": ""
      |            }
      |          ]
      |        }
      |      }
      |    },
      |    {
      |      "name": "status",
      |      "type": [
      |        "null",
      |        {
      |          "type": "record",
      |          "name": "Status",
      |          "namespace": "status.ns",
      |          "doc": "status doc",
      |          "fields": [
      |            {
      |              "name": "code",
      |              "type": "int",
      |              "default": 0
      |            }
      |          ]
      |        }
      |      ],
      |      "default": null
      |    }
      |  ]
      |}
      |""".stripMargin

  private val schema =
    new Schema.Parser().parse(schemaJson)

  test("removeDefaultField removes all default attributes recursively") {
    val updated = removeDefaultField(schema)

    val text = pretty(updated)

    (text should not).include("\"default\"")

    parseable(updated)
  }

  test("removeNamespace removes all namespace attributes recursively") {
    val updated = removeNamespace(schema)

    val text = pretty(updated)

    (text should not).include("\"namespace\"")

    parseable(updated)
  }

  test("removeDocField removes all doc attributes recursively") {
    val updated = removeDocField(schema)

    val text = pretty(updated)

    (text should not).include("\"doc\"")

    parseable(updated)
  }

  test("replaceNamespace replaces all namespaces recursively") {
    val updated = replaceNamespace(schema, "new.ns")

    updated.getNamespace shouldBe "new.ns"

    updated.getField("address").schema().getNamespace shouldBe "new.ns"

    updated
      .getField("tags")
      .schema()
      .getElementType
      .getNamespace shouldBe "new.ns"

    updated
      .getField("status")
      .schema()
      .getTypes
      .get(1)
      .getNamespace shouldBe "new.ns"

    parseable(updated)
  }

  test("replaceNamespace is idempotent") {
    val once = replaceNamespace(schema, "new.ns")
    val twice = replaceNamespace(once, "new.ns")

    pretty(twice) shouldBe pretty(once)
  }

  test("combined transformations produce a valid schema") {
    val transformed =
      replaceNamespace(
        removeDocField(
          removeDefaultField(schema)
        ),
        "new.ns"
      )

    parseable(transformed)

    val text = pretty(transformed)

    (text should not).include("\"default\"")
    (text should not).include("\"doc\"")
    text should include("\"namespace\" : \"new.ns\"")
  }

  test("removeDefaultField on schema without defaults is a no-op") {
    val simple =
      new Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "Simple",
          |  "fields": [
          |    {
          |      "name": "id",
          |      "type": "string"
          |    }
          |  ]
          |}
          |""".stripMargin
      )

    pretty(removeDefaultField(simple)) shouldBe pretty(simple)
  }

  test("removeNamespace on primitive schema is a no-op") {
    val primitive = Schema.create(Schema.Type.STRING)

    removeNamespace(primitive) shouldBe primitive
  }

  test("removeDocField on primitive schema is a no-op") {
    val primitive = Schema.create(Schema.Type.INT)

    removeDocField(primitive) shouldBe primitive
  }

  test("replaceNamespace on schema without namespace does not fail") {
    val simple =
      new Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "Simple",
          |  "fields": [
          |    {
          |      "name": "id",
          |      "type": "string"
          |    }
          |  ]
          |}
          |""".stripMargin
      )

    noException should be thrownBy
      replaceNamespace(simple, "new.ns")
  }
}
