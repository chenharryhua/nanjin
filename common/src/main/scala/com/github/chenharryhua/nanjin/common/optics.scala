package com.github.chenharryhua.nanjin.common
import cats.implicits.{toFunctorOps, toTraverseOps}
import cats.kernel.Monoid
import cats.{Applicative, Foldable, Traverse}
import io.circe.{Json, JsonObject}
import monocle.function.*
import monocle.{Fold, Lens, Traversal}

// copy from https://github.com/circe/circe-optics/blob/master/optics/src/main/scala/io/circe/optics

object optics {

  final lazy val jsonObjectFields: Fold[JsonObject, (String, Json)] = new Fold[JsonObject, (String, Json)] {
    def foldMap[M: Monoid](f: ((String, Json)) => M)(obj: JsonObject): M =
      Foldable[List].foldMap(obj.toList)(f)
  }

  implicit final lazy val jsonObjectEach: Each[JsonObject, Json] = new Each[JsonObject, Json] {
    final def each: Traversal[JsonObject, Json] = new Traversal[JsonObject, Json] {
      final def modifyA[F[_]](f: Json => F[Json])(from: JsonObject)(implicit
        F: Applicative[F]): F[JsonObject] = from.traverse(f)
    }
  }

  implicit final lazy val jsonObjectAt: At[JsonObject, String, Option[Json]] =
    (field: String) =>
      Lens[JsonObject, Option[Json]](_.apply(field))(optVal =>
        obj => optVal.fold(obj.remove(field))(value => obj.add(field, value)))

  implicit final lazy val jsonObjectFilterIndex: FilterIndex[JsonObject, String, Json] =
    (p: String => Boolean) =>
      new Traversal[JsonObject, Json] {
        final def modifyA[F[_]](f: Json => F[Json])(from: JsonObject)(implicit
          F: Applicative[F]
        ): F[JsonObject] =
          F.map(
            Traverse[List].traverse(from.toList) { case (field, json) =>
              F.map(if (p(field)) f(json) else F.point(json))((field, _))
            }
          )(JsonObject.fromFoldable(_))
      }

  implicit final lazy val jsonObjectIndex: Index[JsonObject, String, Json] = Index.fromAt

  implicit final lazy val jsonPlated: Plated[Json] = new Plated[Json] {
    val plate: Traversal[Json, Json] = new Traversal[Json, Json] {
      def modifyA[F[_]](f: Json => F[Json])(a: Json)(implicit
        F: Applicative[F]
      ): F[Json] =
        a.fold(
          F.pure(a),
          b => F.pure(Json.fromBoolean(b)),
          n => F.pure(Json.fromJsonNumber(n)),
          s => F.pure(Json.fromString(s)),
          _.traverse(f).map(Json.fromValues),
          _.traverse(f).map(Json.fromJsonObject)
        )
    }
  }

  final lazy val jsonDescendants: Traversal[Json, Json] = new Traversal[Json, Json] {
    override def modifyA[F[_]](f: Json => F[Json])(s: Json)(implicit F: Applicative[F]): F[Json] =
      s.fold(
        F.pure(s),
        _ => F.pure(s),
        _ => F.pure(s),
        _ => F.pure(s),
        arr => F.map(Each.each[Vector[Json], Json].modifyA(f)(arr))(Json.arr(_*)),
        obj => F.map(Each.each[JsonObject, Json].modifyA(f)(obj))(Json.fromJsonObject)
      )
  }
}
