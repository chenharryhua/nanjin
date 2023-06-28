package com.github.chenharryhua.nanjin.common
import cats.Applicative
import cats.implicits.{toFunctorOps, toTraverseOps}
import io.circe.Json
import monocle.Traversal
import monocle.function.Plated

// copy from https://github.com/circe/circe-optics/blob/master/optics/src/main/scala/io/circe/optics/JsonOptics.scala

object optics {
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
}
