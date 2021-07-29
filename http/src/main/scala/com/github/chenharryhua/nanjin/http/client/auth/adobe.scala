package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.Uri.Path.Segment
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Credentials, Uri, UrlForm}
import org.typelevel.ci.CIString

import java.lang.Boolean.TRUE
import java.security.PrivateKey
import java.util.Date
import scala.collection.JavaConverters.*
import scala.concurrent.duration.DurationLong

object adobe {

  final class IMS[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_code: String,
    client_secret: String,
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, IMS[F]] with UpdateConfig[AuthConfig, IMS[F]] {
    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.millisecond

    val params: AuthParams = config.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val getToken: F[Token] =
        params
          .authClient(client)
          .expect[Token](POST(
            UrlForm(
              "grant_type" -> "authorization_code",
              "client_id" -> client_id,
              "client_secret" -> client_secret,
              "code" -> client_code),
            auth_endpoint.withPath(path"/ims/token/v1")
          ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Either[Throwable, Token]]): F[Unit] = for {
        newToken <- ref.get.flatMap {
          case Left(_)      => getToken.delayBy(params.whenNext).attempt
          case Right(value) => getToken.delayBy(params.whenNext(value)).attempt
        }
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.attempt.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- Resource.eval(middleware(client))
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get.rethrow)
          out <- c.run(
            req.putHeaders(
              Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
              "x-api-key" -> client_id))
        } yield out
      }
    }

    override def updateConfig(f: AuthConfig => AuthConfig): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        config = f(config),
        middleware = middleware)

    override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        config = config,
        middleware = compose(f, middleware))
  }

  object IMS {
    def apply[F[_]](auth_endpoint: Uri, client_id: String, client_code: String, client_secret: String)(implicit
      F: Applicative[F]): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        config = AuthConfig(2.hours),
        middleware = Kleisli(F.pure))
  }

  // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
  final class JWT[F[_]] private (
    auth_endpoint: Uri,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    metascopes: NonEmptyList[AdobeMetascope],
    private_key: PrivateKey,
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, JWT[F]] with UpdateConfig[AuthConfig, JWT[F]] {
    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.millisecond

    val params: AuthParams = config.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
      val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
        auth_endpoint.withPath(path"s" / Segment(ms.name)).renderString -> (TRUE: AnyRef)
      }.toList.toMap.asJava

      val getToken: F[Token] =
        F.realTimeInstant.map { ts =>
          Jwts.builder
            .setSubject(technical_account_key)
            .setIssuer(ims_org_id)
            .setAudience(audience)
            .setExpiration(Date.from(ts.plusSeconds(params.tokenExpiresIn.toSeconds)))
            .addClaims(claims)
            .signWith(private_key, SignatureAlgorithm.RS256)
            .compact
        }.flatMap(jwt =>
          params
            .authClient(client)
            .expect[Token](
              POST(
                UrlForm("client_id" -> client_id, "client_secret" -> client_secret, "jwt_token" -> jwt),
                auth_endpoint.withPath(path"/ims/exchange/jwt")
              ).putHeaders("Cache-Control" -> "no-cache")))

      def updateToken(ref: Ref[F, Either[Throwable, Token]]): F[Unit] = for {
        newToken <- ref.get.flatMap {
          case Left(_)      => getToken.delayBy(params.whenNext).attempt
          case Right(value) => getToken.delayBy(params.whenNext(value)).attempt
        }
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.attempt.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- Resource.eval(middleware(client))
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get.rethrow)
          out <- c.run(
            req.putHeaders(
              Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
              "x-gw-ims-org-id" -> ims_org_id,
              "x-api-key" -> client_id))
        } yield out
      }
    }

    override def updateConfig(f: AuthConfig => AuthConfig): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        config = f(config),
        middleware = middleware)

    override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        config = config,
        middleware = compose(f, middleware))
  }

  object JWT {
    def apply[F[_]](
      auth_endpoint: Uri,
      ims_org_id: String,
      client_id: String,
      client_secret: String,
      technical_account_key: String,
      metascopes: NonEmptyList[AdobeMetascope],
      private_key: PrivateKey)(implicit F: Applicative[F]): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        config = AuthConfig(2.hours),
        middleware = Kleisli(F.pure))

    def apply[F[_]: Applicative](
      auth_endpoint: Uri,
      ims_org_id: String,
      client_id: String,
      client_secret: String,
      technical_account_key: String,
      metascope: AdobeMetascope,
      private_key: PrivateKey): JWT[F] =
      apply[F](
        auth_endpoint,
        ims_org_id,
        client_id,
        client_secret,
        technical_account_key,
        NonEmptyList.one(metascope),
        private_key)
  }
}
