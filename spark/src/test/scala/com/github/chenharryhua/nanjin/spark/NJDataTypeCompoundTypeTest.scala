package com.github.chenharryhua.nanjin.spark

import frameless.TypedEncoder
import org.apache.spark.sql.avro.SchemaConverters
import org.scalatest.funsuite.AnyFunSuite

object NJDataTypeCompoundTypeTestData {
  final case class Food(f: Int, g: String)
  final case class FishFood(d: String, e: Option[Food])
  final case class DogFish(a: Array[Int], b: Array[String], c: Array[FishFood])
  val dogfish: TypedEncoder[DogFish] = shapeless.cachedImplicit
}

class NJDataTypeCompoundTypeTest extends AnyFunSuite {
  import NJDataTypeCompoundTypeTestData._
  test("compound type") {
    println(
      SchemaConverters.toAvroType(dogfish.catalystRepr, true, "spark", "sparkns").toString(true))
    println(NJDataTypeF.genSchema(dogfish.catalystRepr))
  }
}
