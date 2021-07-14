package com.github.chenharryhua.nanjin.http.auth

import cats.effect.std.{Hotswap, Supervisor}
import cats.effect.syntax.all.*
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.JsonCodec
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.Retry
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Headers, Response, Uri}

import java.io.File
import java.lang.Boolean.TRUE
import java.security.interfaces.RSAPrivateKey
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

@JsonCodec
final case class AdobeTokenResponse(token_type: String, expires_in: Long, access_token: String)

sealed abstract class AdobeToken(val name: String)

object AdobeToken {

  final case class IMS[F[_]](auth_endpoint: Uri, client_id: String, client_code: String, client_secret: String)
      extends AdobeToken("access_token") with Http4sClientDsl[F] with Login[F]{
   override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: F[AdobeTokenResponse] =
        Retry(authPolicy[F])(client).expect[AdobeTokenResponse](POST(
         auth_endpoint.withPath(path"/ims/token/v1")
          .withQueryParam("grant_type", "authorization_code")
          .withQueryParam("client_id", client_id)
          .withQueryParam("client_secret", client_secret)
          .withQueryParam("code", client_code)
      ).withHeaders("Content-Type" -> "application/x-www-form-urlencoded"))

      Stream.resource(for {
        hotswap <- Hotswap.create[F, Response[F]]
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(
          supervisor.supervise(ref.get.flatMap(t => getToken.delayBy(FiniteDuration(t.expires_in / 2, TimeUnit.SECONDS)).flatMap(ref.set)).foreverM[Unit]))
      } yield Client[F] { req =>
        Resource.eval(ref.get.flatMap(t =>
          hotswap.swap(client.run(req.putHeaders(Headers(
            "Authorization" -> s"${t.token_type} ${t.access_token}",
            "x-api-key" -> client_id))))))
      })
    }
  }

  final case class JWT[F[_]](
    auth_endpoint: Uri,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    private_key: Either[File, Array[Byte]])
      extends AdobeToken("jwt_token") with Http4sClientDsl[F] with Login[F] {
    // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: F[AdobeTokenResponse] =
        F.realTimeInstant.map { ts =>
        val pk: RSAPrivateKey = private_key.fold(encryption.pkcs8, encryption.pkcs8)
        Jwts.builder
          .setSubject(technical_account_key)
          .setIssuer(ims_org_id)
          .setAudience(s"$auth_endpoint/c/$client_id")
          .setExpiration(new Date(ts.plusSeconds(86400).toEpochMilli))
          .claim(s"$auth_endpoint/s/ent_dataservices_sdk", TRUE)
          .signWith(pk, SignatureAlgorithm.RS256)
          .compact
      }.flatMap(jwt =>
        Retry(authPolicy[F])(client).expect[AdobeTokenResponse](
          POST(
             auth_endpoint.withPath(path"/ims/exchange/jwt")
              .withQueryParam("client_id", client_id)
              .withQueryParam("client_secret", client_secret)
              .withQueryParam("jwt_token", jwt))
            .putHeaders("Content-Type" -> "application/x-www-form-urlencoded", "Cache-Control" -> "no-cache")))

      Stream.resource(for {
        hotswap <- Hotswap.create[F, Response[F]]
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(
          supervisor.supervise(ref.get.flatMap(t => getToken.delayBy(FiniteDuration(t.expires_in / 2, TimeUnit.SECONDS)).flatMap(ref.set)).foreverM[Unit]))
      } yield Client[F] { req =>
        Resource.eval(ref.get.flatMap(t =>
          hotswap.swap(client.run(req.putHeaders(Headers(
            "Authorization" -> s"${t.token_type} ${t.access_token}",
            "x-gw-ims-org-id" -> ims_org_id,
            "x-api-key" -> client_id))))))
      })
    }
  }
}
