package com.github.chenharryhua.nanjin.common

import scala.quoted.*

/** Compile-time typeclass providing the short name of a Scala type.
  *
  * The name is the type symbol's own name, so type arguments are dropped: `TypeName[List[Int]].value` is
  * `"List"`, not `"List[Int]"`.
  *
  * {{{
  * TypeName[String].value // "String"
  * }}}
  */
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
