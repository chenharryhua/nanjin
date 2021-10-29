package com.github.chenharryhua.nanjin.spark

import avrohugger.Generator
import avrohugger.format.Standard
import avrohugger.types.{AvroScalaTypes, ScalaArray}
import frameless.TypedEncoder
import org.scalatest.funsuite.AnyFunSuite

object NJDataTypeCompoundTypeTestData {
  final case class Food(f: Int, g: Array[Byte])
  final case class FishFood(d: String, e: Option[Food])
  final case class DogFish(a: Array[Int], b: Array[String], c: Array[FishFood])
  val dogfish: TypedEncoder[DogFish] = shapeless.cachedImplicit

  // map key must be string.
  // https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html#avro-complexdatatypes
  final case class Squalidae(a: Int, b: Map[String, Int], c: Map[String, List[FishFood]])
  val squalidae: TypedEncoder[Squalidae] = shapeless.cachedImplicit

}

class NJDataTypeCompoundTypeTest extends AnyFunSuite {
  import NJDataTypeCompoundTypeTestData._

  val generator: Generator =
    Generator(Standard, avroScalaCustomTypes = Some(AvroScalaTypes(array = ScalaArray)))

  test("array type") {
    val s = NJDataType(dogfish.catalystRepr).toSchema
    println(dogfish.catalystRepr)
    println(s)
    println(generator.schemaToStrings(s))
  }

  test("map type") {
    val s = NJDataType(squalidae.catalystRepr).toSchema
    println(squalidae.catalystRepr)
    println(s)
    println(generator.schemaToStrings(s))
  }
}
