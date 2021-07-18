package com.github.chenharryhua.nanjin.http.auth

import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.auto.*
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.Uri.Path.Segment
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Headers, Uri, UrlForm}

import java.lang.Boolean.TRUE
import java.security.PrivateKey
import java.util.Date
import scala.collection.JavaConverters.*
import scala.concurrent.duration.{DurationLong, FiniteDuration}

sealed abstract class AdobeToken(val name: String)

object AdobeToken {

  final private case class TokenResponse(
    token_type: String,
    expires_in: Long, // in milliseconds
    access_token: String)

  final class IMS[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_code: String,
    client_secret: String,
    config: AuthConfig)
      extends AdobeToken("access_token") with Http4sClientDsl[F] with Login[F] {

    val params: AuthParams = config.evalConfig

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, TokenResponse] =
        Stream.eval(
          params
            .authClient(client)
            .expect[TokenResponse](POST(
              UrlForm(
                "grant_type" -> "authorization_code",
                "client_id" -> client_id,
                "client_secret" -> client_secret,
                "code" -> client_code),
              auth_endpoint.withPath(path"/ims/token/v1")
            ).putHeaders("Cache-Control" -> "no-cache")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream
            .eval(token.get)
            .flatMap(t => getToken.delayBy(params.offset(t.expires_in.millisecond)).evalMap(token.set))
            .repeat
        Stream[F, Client[F]](Client[F] { req =>
          Resource
            .eval(token.get)
            .flatMap(t =>
              client.run(req.putHeaders(
                Headers("Authorization" -> s"${t.token_type} ${t.access_token}", "x-api-key" -> client_id))))
        }).concurrently(refresh)
      }
    }

    private def updateConfig(f: AuthConfig => AuthConfig): IMS[F] =
      new IMS[F](auth_endpoint, client_id, client_code, client_secret, f(config))

    def withMaxRetries(times: Int): IMS[F]       = updateConfig(_.withMaxRetries(times))
    def withMaxWait(dur: FiniteDuration): IMS[F] = updateConfig(_.withMaxWait(dur))

  }
  object IMS {
    def apply[F[_]](auth_endpoint: Uri, client_id: String, client_code: String, client_secret: String): IMS[F] =
      new IMS[F](auth_endpoint, client_id, client_code, client_secret, AuthConfig(0.seconds))
  }

  // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
  final class JWT[F[_]](
    auth_endpoint: Uri,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    metascopes: NonEmptyList[AdobeMetascope],
    private_key: PrivateKey,
    config: AuthConfig)
      extends AdobeToken("jwt_token") with Http4sClientDsl[F] with Login[F] {

    val params: AuthParams = config.evalConfig

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
      val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
        auth_endpoint.withPath(path"s" / Segment(ms.name)).renderString -> (TRUE: AnyRef)
      }.toList.toMap.asJava

      val getToken: Stream[F, TokenResponse] =
        Stream.eval(
          F.realTimeInstant.map { ts =>
            Jwts.builder
              .setSubject(technical_account_key)
              .setIssuer(ims_org_id)
              .setAudience(audience)
              .setExpiration(Date.from(ts.plusSeconds(params.expiresIn.toSeconds)))
              .addClaims(claims)
              .signWith(private_key, SignatureAlgorithm.RS256)
              .compact
          }.flatMap(jwt =>
            params
              .authClient(client)
              .expect[TokenResponse](
                POST(
                  UrlForm("client_id" -> client_id, "client_secret" -> client_secret, "jwt_token" -> jwt),
                  auth_endpoint.withPath(path"/ims/exchange/jwt")
                ).putHeaders("Cache-Control" -> "no-cache"))))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream
            .eval(token.get)
            .flatMap(t => getToken.delayBy(params.offset(t.expires_in.millisecond)).evalMap(token.set))
            .repeat
        Stream[F, Client[F]](Client[F] { req =>
          Resource
            .eval(token.get)
            .flatMap(t =>
              client.run(
                req.putHeaders(
                  Headers(
                    "Authorization" -> s"${t.token_type} ${t.access_token}",
                    "x-gw-ims-org-id" -> ims_org_id,
                    "x-api-key" -> client_id))))
        }).concurrently(refresh)
      }
    }

    private def updateConfig(f: AuthConfig => AuthConfig): JWT[F] =
      new JWT[F](
        auth_endpoint,
        ims_org_id,
        client_id,
        client_secret,
        technical_account_key,
        metascopes,
        private_key,
        f(config))

    def withMaxRetries(times: Int): JWT[F]         = updateConfig(_.withMaxRetries(times))
    def withMaxWait(dur: FiniteDuration): JWT[F]   = updateConfig(_.withMaxWait(dur))
    def withExpiresIn(dur: FiniteDuration): JWT[F] = updateConfig(_.withExpiresIn(dur))
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
        auth_endpoint,
        ims_org_id,
        client_id,
        client_secret,
        technical_account_key,
        metascopes,
        private_key,
        AuthConfig(1.day))

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
