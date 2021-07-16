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
import org.http4s.client.middleware.Retry
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Headers, Uri}

import java.lang.Boolean.TRUE
import java.security.PrivateKey
import java.util.Date
import scala.collection.JavaConverters.*
import scala.concurrent.duration.*

sealed abstract class AdobeToken(val name: String)

object AdobeToken {

  final private case class TokenResponse(
    token_type: String,
    expires_in: Long, // in milliseconds
    access_token: String)

  final case class IMS[F[_]](auth_endpoint: Uri, client_id: String, client_code: String, client_secret: String)
      extends AdobeToken("access_token") with Http4sClientDsl[F] with Login[F] {

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, TokenResponse] =
        Stream.eval(
          Retry(authPolicy[F])(client).expect[TokenResponse](
            POST(
              auth_endpoint
                .withPath(path"/ims/token/v1")
                .withQueryParam("grant_type", "authorization_code")
                .withQueryParam("client_id", client_id)
                .withQueryParam("client_secret", client_secret)
                .withQueryParam("code", client_code)
            ).withHeaders("Content-Type" -> "application/x-www-form-urlencoded")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream.eval(token.get).flatMap(t => getToken.delayBy(t.expires_in.millisecond).evalMap(token.set)).repeat
        Stream[F, Client[F]](Client[F] { req =>
          Resource
            .eval(token.get)
            .flatMap(t =>
              client.run(req.putHeaders(
                Headers("Authorization" -> s"${t.token_type} ${t.access_token}", "x-api-key" -> client_id))))
        }).concurrently(refresh)
      }
    }
  }

  // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
  final case class JWT[F[_]](
    auth_endpoint: Uri,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    metascopes: NonEmptyList[AdobeMetascope],
    private_key: PrivateKey)
      extends AdobeToken("jwt_token") with Http4sClientDsl[F] with Login[F] {

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val audience: String = auth_endpoint.withPath(path"c" / Segment(client_id)).renderString
      val claims: java.util.Map[String, AnyRef] = metascopes.map { ms =>
        (auth_endpoint.withPath(path"s" / Segment(ms.name)).renderString, TRUE: AnyRef)
      }.toList.toMap.asJava

      val getToken: Stream[F, TokenResponse] =
        Stream.eval(
          F.realTimeInstant.map { ts =>
            Jwts.builder
              .setSubject(technical_account_key)
              .setIssuer(ims_org_id)
              .setAudience(audience)
              .setExpiration(new Date(ts.plusSeconds(86400).toEpochMilli))
              .addClaims(claims)
              .signWith(private_key, SignatureAlgorithm.RS256)
              .compact
          }.flatMap(jwt =>
            Retry(authPolicy[F])(client).expect[TokenResponse](
              POST(
                auth_endpoint
                  .withPath(path"/ims/exchange/jwt")
                  .withQueryParam("client_id", client_id)
                  .withQueryParam("client_secret", client_secret)
                  .withQueryParam("jwt_token", jwt))
                .putHeaders("Content-Type" -> "application/x-www-form-urlencoded", "Cache-Control" -> "no-cache"))))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream.eval(token.get).flatMap(t => getToken.delayBy(t.expires_in.millisecond).evalMap(token.set)).repeat
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
  }
}
