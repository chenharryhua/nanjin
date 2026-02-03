package com.github.chenharryhua.nanjin.common

import cats.{Applicative, Traverse}
import cats.syntax.all.*
import higherkindness.droste.data.{Attr, Coattr, Fix}
import monocle.Traversal
import monocle.function.Plated

/** relate Monocle to Droste
  */
object fixpoint extends FixPointTrait

sealed trait FixPointTrait {

  implicit final def platedFix[G[_]: Traverse]: Plated[Fix[G]] =
    Plated[Fix[G]](new Traversal[Fix[G], Fix[G]] {
      override def modifyA[F[_]: Applicative](f: Fix[G] => F[Fix[G]])(s: Fix[G]): F[Fix[G]] =
        Fix.un(s).traverse(f).map(ga => Fix(ga))
    })

  implicit final def platedAttr[G[_]: Traverse, A]: Plated[Attr[G, A]] =
    Plated[Attr[G, A]](new Traversal[Attr[G, A], Attr[G, A]] {
      override def modifyA[F[_]: Applicative](
        f: Attr[G, A] => F[Attr[G, A]])(s: Attr[G, A]): F[Attr[G, A]] = {
        val (a, ga) = Attr.un(s)
        ga.traverse(f).map(Attr(a, _))
      }
    })

  implicit final def platedCoattr[G[_]: Traverse, A]: Plated[Coattr[G, A]] =
    Plated[Coattr[G, A]](new Traversal[Coattr[G, A], Coattr[G, A]] {
      override def modifyA[F[_]: Applicative](f: Coattr[G, A] => F[Coattr[G, A]])(
        s: Coattr[G, A]): F[Coattr[G, A]] = Coattr.un(s) match {
        case a @ Left(_) => Applicative[F].pure(Coattr(a))
        case Right(ga)   => ga.traverse(f).map(gga => Coattr(Right(gga)))
      }
    })
}
