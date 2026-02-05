package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.implicits.genTemporalOps_
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import io.circe.generic.auto.*
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString

import scala.concurrent.duration.{DurationLong, FiniteDuration}

object Salesforce {

  private class PasswordGrantAuth[F[_]: Async](
    credential: PasswordGrant,
    expiresIn: FiniteDuration,
    authClient: Resource[F, Client[F]]
  ) extends Login[F] {

    private val urlForm: UrlForm = UrlForm(
      "grant_type" -> "password",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret,
      "username" -> credential.username,
      "password" -> credential.password
    )

    private case class Token(
      access_token: String,
      instance_url: String,
      id: String,
      token_type: String,
      issued_at: String,
      signature: String)

    override def loginR(client: Client[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val tac = new TokenAuthClient[F, Token] {
          override protected def getToken: F[Token] =
            post_token[Token](authenticationClient, credential.auth_endpoint, urlForm)

          override protected def renewToken(ref: Ref[F, Token]): F[Unit] =
            getToken.delayBy(expiresIn).flatMap(ref.set)

          override protected def withToken(token: Token, req: Request[F]): Request[F] =
            req
              .withUri(Uri.unsafeFromString(token.instance_url).withPath(req.pathInfo))
              .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
        }

        tac.wrap(client)
      }
  }

  final case class PasswordGrant(
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    username: String,
    password: String)

  def apply[F[_]: Async](
    authClient: Resource[F, Client[F]],
    credential: PasswordGrant,
    expiresIn: FiniteDuration = 2.hours): Login[F] =
    new PasswordGrantAuth[F](
      credential = credential,
      expiresIn = expiresIn,
      authClient = authClient
    )
}
