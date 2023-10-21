package com.github.chenharryhua.nanjin.http.client.auth

import cats.Endo
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import io.jsonwebtoken.Jwts
import org.http4s.Method.*
import org.http4s.Uri.Path.Segment
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
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
    cfg: AuthConfig)
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

      loginInternal(client, getToken, updateToken, withToken)
    }

    override def updateConfig(f: Endo[AuthConfig]): IMS[F] =
      new IMS[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_code = client_code,
        client_secret = client_secret,
        cfg = f(cfg))

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
        cfg = AuthConfig()
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
    cfg: AuthConfig)
      extends Http4sClientDsl[F] with Login[F, JWT[F]] with UpdateConfig[AuthConfig, JWT[F]] {

    private case class Token(
      token_type: String,
      expires_in: Long, // in milliseconds
      access_token: String)

    private val params: AuthParams = cfg.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
      val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
        auth_endpoint.withPath(path"s" / Segment(ms.entryName)).renderString -> (TRUE: AnyRef)
      }.toList.toMap.asJava

      def getToken(expiresIn: FiniteDuration): F[Token] =
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

      loginInternal(client, getToken(12.hours), updateToken, withToken)
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
        cfg = f(cfg)
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
        cfg = AuthConfig()
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
