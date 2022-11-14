package com.github.chenharryhua.nanjin.aws
import cats.effect.kernel.Async
import cats.syntax.all.*
import io.circe.Json
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.Uri

object ecs {

  private val meta_uri: Option[Uri] =
    sys.env
      .get("ECS_CONTAINER_METADATA_URI_V4")
      .orElse(sys.env.get("ECS_CONTAINER_METADATA_URI"))
      .map(Uri.fromString)
      .flatMap(_.toOption)

  // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4.html
  def container_metadata[F[_]: Async]: F[Json] = meta_uri
    .flatTraverse(addr =>
      EmberClientBuilder.default[F].build.use(_.expect[Json](addr).attempt.map(_.toOption)))
    .map(_.fold(Json.Null)(identity))

  def container_metadata_task[F[_]: Async]: F[Json] = meta_uri
    .flatTraverse(addr =>
      EmberClientBuilder.default[F].build.use(_.expect[Json](addr / "task").attempt.map(_.toOption)))
    .map(_.fold(Json.Null)(identity))
}
