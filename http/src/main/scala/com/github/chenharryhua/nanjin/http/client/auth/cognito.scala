package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import fs2.Stream
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

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {

      val authURI = auth_endpoint.withPath(path"/oauth2/token")
      val getToken: Stream[F, Token] =
        Stream.eval(
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
            ).putHeaders("Cache-Control" -> "no-cache")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream
            .eval(token.get)
            .evalMap { t =>
              params
                .authClient(client)
                .expect[Token](POST(
                  UrlForm(
                    "grant_type" -> "refresh_token",
                    "client_id" -> client_id,
                    "refresh_token" -> t.refresh_token),
                  authURI,
                  Authorization(BasicCredentials(client_id, client_secret))
                ).putHeaders("Cache-Control" -> "no-cache"))
                .delayBy(params.whenNext(t))
            }
            .evalMap(token.set)
            .repeat
        Stream
          .eval(middleware(client))
          .map { client =>
            Client[F] { req =>
              Resource
                .eval(token.get)
                .flatMap(t =>
                  client.run(req.putHeaders(Authorization(Credentials.Token(CIString(t.token_type), t.access_token)))))
            }
          }
          .concurrently(refresh)
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

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, Token] =
        Stream.eval(
          params
            .authClient(client)
            .expect[Token](POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "scope" -> scopes.toList.mkString(" ")
              ),
              auth_endpoint.withPath(path"/oauth2/token"),
              Authorization(BasicCredentials(client_id, client_secret))
            ).putHeaders("Cache-Control" -> "no-cache")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream.eval(token.get).flatMap(t => getToken.delayBy(params.whenNext(t))).evalMap(token.set).repeat

        Stream
          .eval(middleware(client))
          .map { client =>
            Client[F] { req =>
              Resource
                .eval(token.get)
                .flatMap(t =>
                  client.run(req.putHeaders(Authorization(Credentials.Token(CIString(t.token_type), t.access_token)))))
            }
          }
          .concurrently(refresh)
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
