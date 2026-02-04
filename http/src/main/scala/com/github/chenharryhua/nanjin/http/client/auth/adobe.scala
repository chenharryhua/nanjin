package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto.*
import io.jsonwebtoken.Jwts
import org.http4s.Method.*
import org.http4s.Uri.Path.Segment
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Idempotency-Key`, Authorization}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import java.lang.Boolean.TRUE
import java.security.PrivateKey
import java.util.Date
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.*

object adobe {
  // ??? https://developer.adobe.com/developer-console/docs/guides/authentication/IMS/#authorize-request
  final class IMS[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_code: String,
    client_secret: String,
    authClient: Resource[F, Client[F]])
      extends Http4sClientDsl[F] with Login[F] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val get_token: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](
              POST(
                UrlForm(
                  "grant_type" -> "authorization_code",
                  "client_id" -> client_id,
                  "client_secret" -> client_secret,
                  "code" -> client_code),
                auth_endpoint
              ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        def update_token(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            newToken <- get_token.delayBy(oldToken.expires_in.millisecond)
            _ <- ref.set(newToken)
          } yield ()

        def with_token(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(
            Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
            "x-api-key" -> client_id)

        login_internal(client, get_token, update_token, with_token)
      }
  }

  object IMS {
    def apply[F[_]](authClient: Resource[F, Client[F]])(
      auth_endpoint: Uri,
      client_id: String,
      client_code: String,
      client_secret: String): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        authClient = authClient
      )
  }

  // https://developer.adobe.com/developer-console/docs/guides/authentication/JWT/
  final class JWT[F[_]] private (
    auth_endpoint: Uri,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    metascopes: NonEmptyList[AdobeMetascope],
    private_key: PrivateKey,
    authClient: Resource[F, Client[F]])
      extends Http4sClientDsl[F] with Login[F] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
        val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
          auth_endpoint.withPath(path"s" / Segment(ms.entryName)).renderString -> (TRUE: AnyRef)
        }.toList.toMap.asJava

        def getToken(expiresIn: FiniteDuration): F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            F.realTimeInstant.map { ts =>
              Jwts
                .builder()
                .subject(technical_account_key)
                .issuer(ims_org_id)
                .expiration(Date.from(ts.plusSeconds(expiresIn.toSeconds)))
                .claims(claims)
                .signWith(private_key, Jwts.SIG.RS256)
                .audience()
                .add(audience)
                .and()
                .compact()
            }.flatMap(jwt =>
              authenticationClient.expect[Token](
                POST(
                  UrlForm("client_id" -> client_id, "client_secret" -> client_secret, "jwt_token" -> jwt),
                  auth_endpoint.withPath(path"/ims/exchange/jwt")
                ).putHeaders(`Idempotency-Key`(show"$uuid"))))
          }

        def updateToken(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            expiresIn = oldToken.expires_in.millisecond
            newToken <- getToken(expiresIn).delayBy(expiresIn)
            _ <- ref.set(newToken)
          } yield ()

        def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(
            Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
            "x-gw-ims-org-id" -> ims_org_id,
            "x-api-key" -> client_id)

        login_internal(client, getToken(12.hours), updateToken, withToken)
      }
  }

  object JWT {
    def apply[F[_]](authClient: Resource[F, Client[F]])(
      auth_endpoint: Uri,
      ims_org_id: String,
      client_id: String,
      client_secret: String,
      technical_account_key: String,
      metascopes: NonEmptyList[AdobeMetascope],
      private_key: PrivateKey): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        authClient = authClient
      )
  }

  final class OAuth[F[_]] private (credential: OAuth.Credential, authClient: Resource[F, Client[F]])
      extends Http4sClientDsl[F] with Login[F] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in second
      access_token: String)

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val get_token: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "client_id" -> credential.client_id,
                "client_secret" -> credential.client_secret,
                "scope" -> "openid,session,AdobeID,read_organizations,additional_info.projectedProductContext"
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

        def with_token(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

        login_internal(client, get_token, update_token, with_token)
      }
  }

  object OAuth {
    final case class Credential(auth_endpoint: Uri, client_id: String, client_secret: String)

    def apply[F[_]](authClient: Resource[F, Client[F]])(credential: Credential): OAuth[F] =
      new OAuth[F](
        credential = credential,
        authClient = authClient
      )
  }
}
