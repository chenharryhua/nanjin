package com.github.chenharryhua.nanjin.common

import io.github.iltotore.iron.:|
import io.github.iltotore.iron.constraint.numeric.Positive

import scala.quoted.*

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

type ChunkSize = Int :| Positive
