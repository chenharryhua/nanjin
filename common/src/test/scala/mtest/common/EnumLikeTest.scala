package mtest.common

import com.github.chenharryhua.nanjin.common.transformers._
import io.scalaland.chimney.Transformer
import io.scalaland.enumz.Enum
import monocle.Iso

object ScalaEnumTest {

  object Phone extends Enumeration {
    val Mobile, G3, G4 = Value
  }

  final case class Address(street: String, phone: Phone.Value)
  final case class Address2(street: String, phone: String)

  implicit val en: Enum[Phone.Value] = Enum[Phone.Value]

  val addr: Transformer[Address2, Address]  = Transformer.derive[Address2, Address]
  val addr2: Transformer[Address, Address2] = Transformer.derive[Address, Address2]

}

object IsoTest {
  final case class Num(a: Int, b: Float)
  final case class NumStr(a: Int, b: String)

  implicit val iso: Iso[Float, String] = null

  val n1 = Transformer.derive[Num, NumStr]
  val n2 = Transformer.derive[NumStr, Num]

}

object SumTypeTest {
  sealed trait Phone
  case object Mobile extends Phone
  case object G3 extends Phone
  case object G4 extends Phone

  final case class Address(street: String, phone: Phone)
  final case class Address2(street: String, phone: String)

  implicit val en: Enum[Phone] = Enum[Phone]

  val addr  = Transformer.derive[Address2, Address]
  val addr2 = Transformer.derive[Address, Address2]

}
