package com.github.chenharryhua.nanjin.spark

import org.apache.avro.Schema
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

object NJDataTypeFTestData {

  val schemaText: String =
    """
      |{
      |  "type" : "record",
      |  "name" : "FixMe",
      |  "namespace" : "nj.spark",
      |  "fields" : [ {
      |    "name" : "a",
      |    "type" : [ "int", "null" ]
      |  }, {
      |    "name" : "b",
      |    "type" : [ "int", "null" ]
      |  }, {
      |    "name" : "c",
      |    "type" : "int"
      |  }, {
      |    "name" : "d",
      |    "type" : "long"
      |  }, {
      |    "name" : "e",
      |    "type" : "float"
      |  }, {
      |    "name" : "f",
      |    "type" : "double"
      |  }, {
      |    "name" : "g",
      |    "type" : {
      |      "type" : "bytes",
      |      "logicalType" : "decimal",
      |      "precision" : 7,
      |      "scale" : 3
      |    }
      |  }, {
      |    "name" : "h",
      |    "type" : "boolean"
      |  }, {
      |    "name" : "i",
      |    "type" : "bytes"
      |  }, {
      |    "name" : "j",
      |    "type" : "string"
      |  }, {
      |    "name" : "k",
      |    "type" : {
      |      "type" : "long",
      |      "logicalType" : "timestamp-micros"
      |    }
      |  }, {
      |    "name" : "l",
      |    "type" : {
      |      "type" : "int",
      |      "logicalType" : "date"
      |    }
      |  } ]
      |}
      |""".stripMargin

  val schema: Schema = (new Schema.Parser).parse(schemaText)
}

class NJDataTypePrimitiveTypeTest extends AnyFunSuite {
  import NJDataTypeFTestData._
  test("primitive types") {
    val st = StructType(
      List(
        StructField("a", ByteType, true),
        StructField("b", ShortType, true),
        StructField("c", IntegerType, false),
        StructField("d", LongType, false),
        StructField("e", FloatType, false),
        StructField("f", DoubleType, false),
        StructField("g", DecimalType(7, 3), false),
        StructField("h", BooleanType, false),
        StructField("i", BinaryType, false),
        StructField("j", StringType, false),
        StructField("k", TimestampType, false),
        StructField("l", DateType, false)
      )
    )
    assert(NJDataTypeF.genSchema(st) == schema)
    // val spark = SchemaConverters.toAvroType(st, false, "FixMe", "nj.spark")
    // println(spark.toString(true))
  }
}
