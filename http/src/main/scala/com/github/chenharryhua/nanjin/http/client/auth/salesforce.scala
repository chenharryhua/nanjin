package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.implicits.genTemporalOps_
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto.*
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Idempotency-Key`, Authorization}
import org.typelevel.ci.CIString

import scala.concurrent.duration.{DurationLong, FiniteDuration}

object salesforce {

  // ??? https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm

  sealed private trait InstanceURL

  private case object Rest extends InstanceURL

  private case object Soap extends InstanceURL

  final class MarketingCloud[F[_]] private (
    credential: MarketingCloud.Credential,
    instanceURL: InstanceURL,
    authClient: Resource[F, Client[F]]
  ) extends Http4sClientDsl[F] with Login[F] {

    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Long, // in seconds
      scope: String,
      soap_instance_url: String,
      rest_instance_url: String)

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val get_token: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](
              POST(
                UrlForm(
                  "grant_type" -> "client_credentials",
                  "client_id" -> credential.client_id,
                  "client_secret" -> credential.client_secret
                ),
                credential.auth_endpoint
              ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        def update_token(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            newToken <- get_token.delayBy(oldToken.expires_in.seconds)
            _ <- ref.set(newToken)
          } yield ()

        def with_token(token: Token, req: Request[F]): Request[F] = {
          val iu: Uri = instanceURL match {
            case Rest => Uri.unsafeFromString(token.rest_instance_url).withPath(req.pathInfo)
            case Soap => Uri.unsafeFromString(token.soap_instance_url).withPath(req.pathInfo)
          }
          req
            .withUri(iu)
            .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
        }

        login_internal(client, get_token, update_token, with_token)
      }
  }

  object MarketingCloud {

    final case class Credential(auth_endpoint: Uri, client_id: String, client_secret: String)

    def rest[F[_]](authClient: Resource[F, Client[F]])(credential: Credential): MarketingCloud[F] =
      new MarketingCloud[F](
        credential = credential,
        instanceURL = Rest,
        authClient = authClient
      )

    def soap[F[_]](authClient: Resource[F, Client[F]])(credential: Credential): MarketingCloud[F] =
      new MarketingCloud[F](
        credential = credential,
        instanceURL = Soap,
        authClient = authClient
      )
  }

  // https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
  final class Iot[F[_]] private (
    credential: Iot.Credential,
    expiresIn: FiniteDuration,
    authClient: Resource[F, Client[F]]
  ) extends Http4sClientDsl[F] with Login[F] {

    private case class Token(
      access_token: String,
      instance_url: String,
      id: String,
      token_type: String,
      issued_at: String,
      signature: String)

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val get_token: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "password",
                "client_id" -> credential.client_id,
                "client_secret" -> credential.client_secret,
                "username" -> credential.username,
                "password" -> credential.password
              ),
              credential.auth_endpoint
            ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        def update_token(ref: Ref[F, Token]): F[Unit] =
          get_token.delayBy(expiresIn).flatMap(ref.set)

        def with_token(token: Token, req: Request[F]): Request[F] =
          req
            .withUri(Uri.unsafeFromString(token.instance_url).withPath(req.pathInfo))
            .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

        login_internal(client, get_token, update_token, with_token)
      }
  }

  object Iot {

    final case class Credential(
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      username: String,
      password: String)

    def apply[F[_]](authClient: Resource[F, Client[F]])(
      credential: Credential,
      expiresIn: FiniteDuration = 24.hours): Iot[F] =
      new Iot[F](credential, expiresIn, authClient)
  }
}
