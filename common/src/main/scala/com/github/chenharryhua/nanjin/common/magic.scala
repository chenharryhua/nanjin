package com.github.chenharryhua.nanjin.common

import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.quoted.*

/** Compile-time typeclass providing the short name of a Scala type. */
trait TypeName[A]:
  def value: String
end TypeName

object TypeName:
  def apply[A](using tn: TypeName[A]): TypeName[A] = tn

  inline given [A]: TypeName[A] = ${ typeNameImpl[A] }

  private def typeNameImpl[A: Type](using Quotes): Expr[TypeName[A]] =
    import quotes.reflect.*
    '{ new TypeName[A] { def value: String = ${ Expr(TypeRepr.of[A].typeSymbol.name) } } }
end TypeName

object OpaqueLift:
  /** Lift any typeclass from representation type `B` to opaque type `A` */
  inline def lift[A, B, TC[_]](using tc: TC[B]): TC[A] =
    tc.asInstanceOf[TC[A]] // scalafix:ok
end OpaqueLift

object FieldNames:
  /** Case-class field names, extracted at compile time from the product mirror's element labels, in
    * declaration order.
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
