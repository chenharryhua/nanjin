package com.github.chenharryhua.nanjin.common

import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror

object FieldNames:
  /** Case-class field names, extracted at compile time from the product mirror's element labels, in
    * declaration order.
    *
    * These are the declared Scala names, not any renamed JSON keys: a circe `@JsonKey` or a custom codec
    * mapping is not reflected here.
    *
    * {{{
    * final case class Tiger(name: String, age: Int, colour: String)
    *
    * FieldNames.of[Tiger] // List("name", "age", "colour")
    * }}}
    */
  inline def of[A](using m: Mirror.ProductOf[A]): List[String] = labels[m.MirroredElemLabels]

  private inline def labels[T <: Tuple]: List[String] =
    inline erasedValue[T] match {
      case _: EmptyTuple     => Nil
      case _: (name *: rest) => constValue[name & String] :: labels[rest]
    }
end FieldNames
