package mtest.common

import cats.Show
import com.github.chenharryhua.nanjin.common.{FieldNames, OpaqueLift, TypeName}
import org.scalatest.funsuite.AnyFunSuite

object MagicTest {
  opaque type WrappedInt = Int
  object WrappedInt {
    def apply(i: Int): WrappedInt = i
    given Show[WrappedInt] = OpaqueLift.lift[WrappedInt, Int, Show](using Show.fromToString)
  }
}

class MagicTest extends AnyFunSuite {
  import MagicTest.*

  test("1.TypeName - resolves simple types") {
    assert(TypeName[String].value == "String")
    assert(TypeName[Int].value == "Int")
    assert(TypeName[Boolean].value == "Boolean")
  }

  test("2.TypeName - resolves custom class") {
    class MyCustomClass
    assert(TypeName[MyCustomClass].value == "MyCustomClass")
  }

  test("3.TypeName - resolves case class") {
    case class Person(name: String, age: Int)
    assert(TypeName[Person].value == "Person")
  }

  test("4.TypeName - resolves trait") {
    trait Describable
    assert(TypeName[Describable].value == "Describable")
  }

  test("5.TypeName - resolves generic type by its symbol name") {
    assert(TypeName[List[Int]].value == "List")
    assert(TypeName[Option[String]].value == "Option")
    assert(TypeName[Map[String, Int]].value == "Map")
  }

  test("6.OpaqueLift - lifts a typeclass instance to opaque type") {
    val liftedShow = WrappedInt.given_Show_WrappedInt
    val value = WrappedInt(42)
    assert(liftedShow.show(value) == "42")
  }

  test("7.FieldNames - returns field names in declaration order") {
    case class Tiger(name: String, age: Int, colour: String)
    assert(FieldNames.of[Tiger] == List("name", "age", "colour"))
  }

  test("8.FieldNames - single-field case class") {
    case class Wrapper(value: String)
    assert(FieldNames.of[Wrapper] == List("value"))
  }

  test("9.FieldNames - order is preserved, not sorted") {
    case class Ordered(zebra: Int, apple: Int, mango: Int)
    assert(FieldNames.of[Ordered] == List("zebra", "apple", "mango"))
  }

  test("10.FieldNames - empty case class yields Nil") {
    case class Empty()
    assert(FieldNames.of[Empty] == Nil)
  }
}
