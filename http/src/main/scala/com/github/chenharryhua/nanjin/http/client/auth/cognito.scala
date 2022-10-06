package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.{NonEmptyList, Reader}
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import org.http4s.{BasicCredentials, Credentials, Request, Uri, UrlForm}
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

/** https://docs.aws.amazon.com/cognito/latest/developerguide/token-endpoint.html
  */

object cognito {

  final class AuthorizationCode[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    code: String,
    redirect_uri: String,
    code_verifier: String,
    cfg: AuthConfig,
    middleware: Reader[Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, AuthorizationCode[F]]
      with UpdateConfig[AuthConfig, AuthorizationCode[F]] {

    private case class Token(
      access_token: String,
      refresh_token: String,
      id_token: String,
      token_type: String,
      expires_in: Int // in second
    )

    private val params: AuthParams = cfg.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {

      val authURI = auth_endpoint.withPath(path"/oauth2/token")
      val getToken: F[Token] =
        params
          .authClient(client)
          .expect[Token](POST(
            UrlForm(
              "grant_type" -> "authorization_code",
              "client_id" -> client_id,
              "code" -> code,
              "redirect_uri" -> redirect_uri,
              "code_verifier" -> code_verifier
            ),
            authURI,
            Authorization(BasicCredentials(client_id, client_secret))
          ))

      def refreshToken(pre: Token): F[Token] =
        params
          .authClient(client)
          .expect[Token](POST(
            UrlForm(
              "grant_type" -> "refresh_token",
              "client_id" -> client_id,
              "refresh_token" -> pre.refresh_token),
            authURI,
            Authorization(BasicCredentials(client_id, client_secret))
          ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- refreshToken(oldToken).delayBy(params.dormant(oldToken.expires_in.seconds))
          _ <- ref.set(newToken)
        } yield ()

      def withToken(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

      buildClient(client, getToken, updateToken, withToken).map(middleware.run)
    }

    override def withMiddleware(f: Client[F] => Client[F]): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        cfg = cfg,
        middleware = middleware.compose(f)
      )

    override def updateConfig(f: Endo[AuthConfig]): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        cfg = f(cfg),
        middleware = middleware
      )
  }

  object AuthorizationCode {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      code: String,
      redirect_uri: String,
      code_verifier: String): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        cfg = AuthConfig(),
        middleware = Reader(identity)
      )

  }

  final class ClientCredentials[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    scopes: NonEmptyList[String],
    cfg: AuthConfig,
    middleware: Reader[Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, ClientCredentials[F]]
      with UpdateConfig[AuthConfig, ClientCredentials[F]] {

    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Int // in second
    )

    private val params: AuthParams = cfg.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val getToken: F[Token] =
        params
          .authClient(client)
          .expect[Token](POST(
            UrlForm(
              "grant_type" -> "client_credentials",
              "scope" -> scopes.toList.mkString(" ")
            ),
            auth_endpoint.withPath(path"/oauth2/token"),
            Authorization(BasicCredentials(client_id, client_secret))
          ))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- getToken.delayBy(params.dormant(oldToken.expires_in.seconds))
          _ <- ref.set(newToken)
        } yield ()

      def withToken(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

      buildClient(client, getToken, updateToken, withToken).map(middleware.run)
    }

    override def withMiddleware(f: Client[F] => Client[F]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        cfg = cfg,
        middleware = middleware.compose(f)
      )

    override def updateConfig(f: Endo[AuthConfig]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        cfg = f(cfg),
        middleware = middleware
      )
  }

  object ClientCredentials {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      scopes: NonEmptyList[String]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        cfg = AuthConfig(),
        middleware = Reader(identity))

    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      scope: String): ClientCredentials[F] =
      apply(auth_endpoint, client_id, client_secret, NonEmptyList.one(scope))
  }
}
