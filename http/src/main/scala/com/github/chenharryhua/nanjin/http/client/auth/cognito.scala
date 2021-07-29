package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{BasicCredentials, Credentials, Uri, UrlForm}
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
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, AuthorizationCode[F]]
      with UpdateConfig[AuthConfig, AuthorizationCode[F]] {

    private case class Token(
      access_token: String,
      refresh_token: String,
      id_token: String,
      token_type: String,
      expires_in: Int // in second
    )
    implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.seconds

    val params: AuthParams = config.evalConfig

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
          ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Token]): F[Unit] = for {
        old <- ref.get
        newToken <- params
          .authClient(client)
          .expect[Token](POST(
            UrlForm("grant_type" -> "refresh_token", "client_id" -> client_id, "refresh_token" -> old.refresh_token),
            authURI,
            Authorization(BasicCredentials(client_id, client_secret))
          ).putHeaders("Cache-Control" -> "no-cache"))
          .delayBy(params.whenNext(old))
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- Resource.eval(middleware(client))
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get)
          out <- c.run(req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
        } yield out
      }
    }

    override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        config = config,
        middleware = compose(f, middleware))

    override def updateConfig(f: AuthConfig => AuthConfig): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        config = f(config),
        middleware = middleware)
  }

  object AuthorizationCode {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      code: String,
      redirect_uri: String,
      code_verifier: String)(implicit F: Applicative[F]) =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        config = AuthConfig(1.day),
        Kleisli(F.pure))
  }

  final class ClientCredentials[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    scopes: NonEmptyList[String],
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, ClientCredentials[F]]
      with UpdateConfig[AuthConfig, ClientCredentials[F]] {
    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Int // in second
    )
    implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.seconds

    val params: AuthParams = config.evalConfig

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
          ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Token]): F[Unit] = for {
        old <- ref.get
        newToken <- getToken.delayBy(params.whenNext(old))
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- Resource.eval(middleware(client))
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get)
          out <- c.run(req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
        } yield out
      }
    }

    override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        config = config,
        compose(f, middleware)
      )

    override def updateConfig(f: AuthConfig => AuthConfig): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        config = f(config),
        middleware
      )
  }

  object ClientCredentials {
    def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String, scopes: NonEmptyList[String])(implicit
      F: Applicative[F]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        config = AuthConfig(1.day),
        Kleisli(F.pure)
      )

    def apply[F[_]: Applicative](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      scope: String): ClientCredentials[F] =
      apply(auth_endpoint, client_id, client_secret, NonEmptyList.one(scope))
  }
}
