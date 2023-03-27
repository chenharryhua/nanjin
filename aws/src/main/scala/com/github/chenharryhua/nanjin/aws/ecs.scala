package com.github.chenharryhua.nanjin.aws
import cats.effect.kernel.{Async, Sync}
import cats.effect.std.Env
import cats.syntax.all.*
import io.circe.Json
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.Uri
import fs2.io.net.Network

object ecs {

  private def meta_uri[F[_]: Sync]: F[Option[Uri]] = {
    val env: Env[F] = Env.make[F]
    for {
      v4 <- env.get("ECS_CONTAINER_METADATA_URI_V4")
      v3 <- env.get("ECS_CONTAINER_METADATA_URI")
    } yield v4.orElse(v3).map(Uri.fromString).flatMap(_.toOption)
  }

  // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4.html
  def container_metadata[F[_]: Async: Network]: F[Json] =
    EmberClientBuilder.default[F].build.use { client =>
      for {
        uri <- meta_uri[F]
        json <- uri.flatTraverse(addr => client.expect[Json](addr).attempt.map(_.toOption))
      } yield json.getOrElse(Json.Null)
    }

  def container_metadata_task[F[_]: Async: Network]: F[Json] =
    EmberClientBuilder.default[F].build.use { client =>
      for {
        uri <- meta_uri[F]
        json <- uri.flatTraverse(addr => client.expect[Json](addr / "task").attempt.map(_.toOption))
      } yield json.getOrElse(Json.Null)
    }
}
