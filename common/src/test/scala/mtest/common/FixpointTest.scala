package mtest.common

import cats.{Functor, Traverse}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.fixpoint
import com.github.chenharryhua.nanjin.common.fixpoint.given
import higherkindness.droste.data.{Attr, Coattr, Fix}
import monocle.function.Plated
import org.scalatest.funsuite.AnyFunSuite

class FixpointTest extends AnyFunSuite {

  // A simple recursive functor for testing
  sealed trait ExprF[A]
  case class LitF[A](value: Int) extends ExprF[A]
  case class AddF[A](left: A, right: A) extends ExprF[A]

  given Functor[ExprF] with {
    def map[A, B](fa: ExprF[A])(f: A => B): ExprF[B] = fa match {
      case LitF(v)    => LitF(v)
      case AddF(l, r) => AddF(f(l), f(r))
    }
  }

  given Traverse[ExprF] with {
    def traverse[G[_]: cats.Applicative, A, B](fa: ExprF[A])(f: A => G[B]): G[ExprF[B]] =
      fa match {
        case LitF(v)    => cats.Applicative[G].pure(LitF(v))
        case AddF(l, r) => (f(l), f(r)).mapN(AddF(_, _))
      }
    def foldLeft[A, B](fa: ExprF[A], b: B)(f: (B, A) => B): B = fa match {
      case LitF(_)    => b
      case AddF(l, r) => f(f(b, l), r)
    }
    def foldRight[A, B](fa: ExprF[A], lb: cats.Eval[B])(f: (A, cats.Eval[B]) => cats.Eval[B]): cats.Eval[B] =
      fa match {
        case LitF(_)    => lb
        case AddF(l, r) => f(l, f(r, lb))
      }
  }

  // Helper to build Fix[ExprF]
  def lit(v: Int): Fix[ExprF] = Fix(LitF(v))
  def add(l: Fix[ExprF], r: Fix[ExprF]): Fix[ExprF] = Fix(AddF(l, r))

  test("1.Plated[Fix] - transform replaces all matching nodes") {
    // (1 + 2) + 3
    val expr = add(add(lit(1), lit(2)), lit(3))

    // Replace all Lit(1) with Lit(10)
    val transformed = Plated.transform[Fix[ExprF]] { node =>
      Fix.un(node) match {
        case LitF(1) => lit(10)
        case _       => node
      }
    }(expr)

    // Verify the transformation happened
    Fix.un(transformed) match {
      case AddF(left, right) =>
        Fix.un(left) match {
          case AddF(ll, lr) =>
            assert(Fix.un(ll) == LitF(10))
            assert(Fix.un(lr) == LitF(2))
          case other => fail(s"expected AddF but got $other")
        }
        assert(Fix.un(right) == LitF(3))
      case other => fail(s"expected AddF but got $other")
    }
  }

  test("2.Plated[Fix] - transform on leaf does nothing extra") {
    val leaf = lit(42)
    val transformed = Plated.transform[Fix[ExprF]](identity)(leaf)
    assert(Fix.un(transformed) == LitF(42))
  }

  test("3.Plated[Attr] - transform annotated tree") {
    // Attr carries an annotation at each node
    val annotatedLeaf: Attr[ExprF, String] = Attr("leaf", LitF[Attr[ExprF, String]](5))
    val annotatedLeaf2: Attr[ExprF, String] = Attr("leaf", LitF[Attr[ExprF, String]](7))
    val annotatedAdd: Attr[ExprF, String] = Attr("add", AddF(annotatedLeaf, annotatedLeaf2))

    // Transform: change annotation of all "leaf" nodes to "visited"
    val transformed = Plated.transform[Attr[ExprF, String]] { node =>
      val (ann, layer) = Attr.un(node)
      if (ann == "leaf") Attr("visited", layer)
      else node
    }(annotatedAdd)

    val (topAnn, topLayer) = Attr.un(transformed)
    assert(topAnn == "add")
    topLayer match {
      case AddF(left, right) =>
        assert(Attr.un(left)._1 == "visited")
        assert(Attr.un(right)._1 == "visited")
      case other => fail(s"expected AddF but got $other")
    }
  }

  test("4.Plated[Coattr] - transform on Pure (Left) is identity") {
    val pure: Coattr[ExprF, Int] = Coattr(Left(42))
    val transformed = Plated.transform[Coattr[ExprF, Int]](identity)(pure)
    assert(Coattr.un(transformed) == Left(42))
  }

  test("5.Plated[Coattr] - transform on Roll (Right) descends into children") {
    val child1: Coattr[ExprF, Int] = Coattr(Left(1))
    val child2: Coattr[ExprF, Int] = Coattr(Left(2))
    val roll: Coattr[ExprF, Int] = Coattr(Right(AddF(child1, child2)))

    // Replace Pure(1) with Pure(99)
    val transformed = Plated.transform[Coattr[ExprF, Int]] { node =>
      Coattr.un(node) match {
        case Left(1) => Coattr(Left(99))
        case _       => node
      }
    }(roll)

    Coattr.un(transformed) match {
      case Right(AddF(l, r)) =>
        assert(Coattr.un(l) == Left(99))
        assert(Coattr.un(r) == Left(2))
      case other => fail(s"expected Right(AddF(...)) but got $other")
    }
  }
}
