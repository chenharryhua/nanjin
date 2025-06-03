package mtest.common

import com.github.chenharryhua.nanjin.common.transformers.*
import io.scalaland.chimney.dsl.*
import org.scalatest.funsuite.AnyFunSuite

object ScalaEnumStringTest {

  object Phone extends Enumeration {
    val Mobile, G3, G4 = Value
  }

  final case class Address(street: String, phone: Phone.Value)
  final case class Address2(street: String, phone: String)

}

object ScalaEnumIntTest {

  object Phone extends Enumeration {
    val Mobile, G3, G4 = Value
  }

  final case class Address(street: String, phone: Phone.Value)
  final case class Address2(street: String, phone: Int)

}

object SumTypeTest {
  sealed trait Phone
  case object Mobile extends Phone
  case object G3 extends Phone
  case object G4 extends Phone

  final case class Address(street: String, phone: Phone)
  final case class Address2(street: String, phone: String)

}

class EnumLikeTest extends AnyFunSuite {
  test("enum string transformation") {
    import ScalaEnumStringTest.*
    val add = Address("stream", Phone.Mobile)
    val add2 = add.transformInto[Address2]
    assert(add2 == Address2("stream", "Mobile"))
  }
  test("enum Int transformation") {
    import ScalaEnumIntTest.*

    val add = Address("stream", Phone.Mobile)
    val add2 = add.transformInto[Address2]
    assert(add2 == Address2("stream", 0))
  }
  test("enum sum transformation") {
    import SumTypeTest.*

    val add = Address("stream", Mobile)
    val add2 = add.transformInto[Address2]
    assert(add2 == Address2("stream", "Mobile"))
  }
}
