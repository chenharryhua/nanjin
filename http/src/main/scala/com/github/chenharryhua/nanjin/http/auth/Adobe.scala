package com.github.chenharryhua.nanjin.http.auth

import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.{Async, Concurrent, Ref, Resource}
import cats.syntax.all.*
import io.circe.generic.JsonCodec
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Header, Headers, Uri}
import org.typelevel.ci.CIString

import java.io.File
import java.lang.Boolean.TRUE
import java.security.interfaces.RSAPrivateKey
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

@JsonCodec
final case class AdobeTokenResponse(token_type: String, expires_in: Long, access_token: String)

sealed abstract class AdobeToken(name: String)

object AdobeToken {

  final case class IMS[F[_]](auth_endpoint: String, client_id: String, client_code: String, client_secret: String)
      extends AdobeToken("access_token") with Http4sClientDsl[F] {
    def login(client: Client[F])(implicit F: Concurrent[F]): F[AdobeTokenResponse] = {
      val req = POST(
        Uri
          .unsafeFromString(s"$auth_endpoint/ims/token/v1")
          .withQueryParam("grant_type", "authorization_code")
          .withQueryParam("client_id", client_id)
          .withQueryParam("client_secret", client_secret)
          .withQueryParam("code", client_code)
      ).withHeaders(Header.Raw(CIString("Content-Type"), "application/x-www-form-urlencoded"))
      client.expect[AdobeTokenResponse](req)
    }
  }

  final case class JWT[F[_]](
    auth_endpoint: String,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    private_key: Either[File, Array[Byte]])
      extends AdobeToken("jwt_token") with Http4sClientDsl[F] {
    // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
    def login(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val jwtToken = F.realTimeInstant.map { ts =>
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
        client.expect[AdobeTokenResponse](
          POST(
            Uri
              .unsafeFromString(s"$auth_endpoint/ims/exchange/jwt")
              .withQueryParam("client_id", client_id)
              .withQueryParam("client_secret", client_secret)
              .withQueryParam("jwt_token", jwt)).putHeaders(
            Header.Raw(CIString("Content-Type"), "application/x-www-form-urlencoded"),
            Header.Raw(CIString("Cache-Control"), "no-cache"))))

      Supervisor[F].flatMap { supervisor =>
        Resource.eval(for {
          ref <- jwtToken.flatMap(F.ref)
          _ <- supervisor.supervise(
            ref.get
              .flatMap(t => jwtToken.delayBy(FiniteDuration(t.expires_in / 2, TimeUnit.SECONDS)).flatMap(ref.set))
              .foreverM[Unit])
        } yield Client[F] { req =>
          val auth_req = ref.get.map(t =>
            req.putHeaders(
              Headers(
                Header.Raw(CIString("Authorization"), s"${t.token_type} ${t.access_token}"),
                Header.Raw(CIString("x-gw-ims-org-id"), ims_org_id),
                Header.Raw(CIString("x-api-key"), client_id)
              )))

          Resource.eval(auth_req).flatMap(client.run)
        })
      }
    }
  }
}
