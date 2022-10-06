package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.{NonEmptyList, Reader}
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.http4s.Method.*
import org.http4s.Uri.Path.Segment
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
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
    cfg: AuthConfig,
    middleware: Reader[Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, IMS[F]] with UpdateConfig[AuthConfig, IMS[F]] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    private val params: AuthParams = cfg.evalConfig

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
          ))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- getToken.delayBy(params.dormant(oldToken.expires_in.millisecond))
          _ <- ref.set(newToken)
        } yield ()

      def withToken(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(
          Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
          "x-api-key" -> client_id)

      buildClient(client, getToken, updateToken, withToken).map(middleware.run)
    }

    override def updateConfig(f: Endo[AuthConfig]): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        cfg = f(cfg),
        middleware = middleware)

    override def withMiddleware(f: Client[F] => Client[F]): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        cfg = cfg,
        middleware = middleware.compose(f)
      )
  }

  object IMS {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_code: String,
      client_secret: String): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        cfg = AuthConfig(),
        middleware = Reader(identity)
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
    cfg: AuthConfig,
    middleware: Reader[Client[F], Client[F]])
      extends Http4sClientDsl[F] with Login[F, JWT[F]] with UpdateConfig[AuthConfig, JWT[F]] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    private val params: AuthParams = cfg.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
      val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
        auth_endpoint.withPath(path"s" / Segment(ms.name)).renderString -> (TRUE: AnyRef)
      }.toList.toMap.asJava

      def getToken(expiresIn: FiniteDuration): F[Token] =
        F.realTimeInstant.map { ts =>
          Jwts.builder
            .setSubject(technical_account_key)
            .setIssuer(ims_org_id)
            .setAudience(audience)
            .setExpiration(Date.from(ts.plusSeconds(expiresIn.toSeconds)))
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
              )))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          expiresIn = oldToken.expires_in.millisecond
          newToken <- getToken(expiresIn).delayBy(params.dormant(expiresIn))
          _ <- ref.set(newToken)
        } yield ()

      def withToken(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(
          Authorization(Credentials.Token(CIString(token.token_type), token.access_token)),
          "x-gw-ims-org-id" -> ims_org_id,
          "x-api-key" -> client_id)

      buildClient(client, getToken(1.day), updateToken, withToken).map(middleware.run)
    }

    override def updateConfig(f: Endo[AuthConfig]): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        cfg = f(cfg),
        middleware = middleware
      )

    override def withMiddleware(f: Client[F] => Client[F]): JWT[F] =
      new JWT[F](
        auth_endpoint = auth_endpoint,
        ims_org_id = ims_org_id,
        client_id = client_id,
        client_secret = client_secret,
        technical_account_key = technical_account_key,
        metascopes = metascopes,
        private_key = private_key,
        cfg = cfg,
        middleware = middleware.compose(f)
      )
  }

  object JWT {
    def apply[F[_]](
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
        cfg = AuthConfig(),
        middleware = Reader(identity)
      )

    def apply[F[_]](
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
