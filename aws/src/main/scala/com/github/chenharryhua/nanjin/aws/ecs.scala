package com.github.chenharryhua.nanjin.aws
import cats.effect.kernel.Async
import cats.syntax.all.*
import io.circe.Json
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder

object ecs {

  def container_metadata[F[_]: Async]: F[Json] = sys.env
    .get("ECS_CONTAINER_METADATA_URI_V4")
    .orElse(sys.env.get("ECS_CONTAINER_METADATA_URI"))
    .flatTraverse(addr =>
      EmberClientBuilder.default[F].build.use(_.expect[Json](addr).attempt.map(_.toOption)))
    .map(_.fold(Json.Null)(identity))
}
