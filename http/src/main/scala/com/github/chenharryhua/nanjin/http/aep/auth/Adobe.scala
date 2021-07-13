package com.github.chenharryhua.nanjin.http.aep.auth
import cats.effect.{Async, Concurrent}
import com.github.chenharryhua.nanjin.http.auth.privateKey
import io.circe.generic.JsonCodec
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.Retry
import org.http4s.{Header, Request, Response, Uri}
import org.typelevel.ci.CIString

import java.io.File
import java.lang.Boolean.TRUE
import java.security.interfaces.RSAPrivateKey
import scala.collection.JavaConverters.*
import scala.concurrent.duration.*

@JsonCodec
final case class AdobeTokenResponse(token_type: String, expires_in: Long, access_token: String)

sealed abstract class AdobeTokenType(name: String)

object AdobeTokenType {

  final case class IMS[F[_]](auth_endpoint: String, client_id: String, client_code: String, client_secret: String)
      extends AdobeTokenType("access_token") with Http4sClientDsl[F] {
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
      extends AdobeTokenType("jwt_token") with Http4sClientDsl[F] {
    private val JWT_EXPIRY_KEY: String         = "exp"
    private val JWT_ISS_KEY: String            = "iss"
    private val JWT_AUD_KEY: String            = "aud"
    private val JWT_SUB_KEY: String            = "sub"
    private val JWT_TOKEN_EXPIRATION_THRESHOLD = 86400L // 24 hours

    def login(client: Client[F])(implicit F: Async[F]): F[AdobeTokenResponse] = {
      val claims: Map[String, AnyRef] = Map(
        JWT_ISS_KEY -> ims_org_id,
        JWT_SUB_KEY -> technical_account_key,
        JWT_EXPIRY_KEY -> new java.lang.Long(System.currentTimeMillis() / 1000 + JWT_TOKEN_EXPIRATION_THRESHOLD),
        JWT_AUD_KEY -> s"$auth_endpoint/c/$client_id",
        s"$auth_endpoint/s/ent_dataservices_sdk" -> TRUE
      )
      val pk: RSAPrivateKey = private_key.fold(privateKey.pkcs8, privateKey.pkcs8)
      val jwtToken          = Jwts.builder.setClaims(claims.asJava).signWith(pk, SignatureAlgorithm.RS256).compact

      // https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/JWT/JWT.md
      val req = POST(
        Uri
          .unsafeFromString(s"$auth_endpoint/ims/exchange/jwt")
          .withQueryParam("client_id", client_id)
          .withQueryParam("client_secret", client_secret)
          .withQueryParam("jwt_token", jwtToken)).putHeaders(
        Header.Raw(CIString("Content-Type"), "application/x-www-form-urlencoded"),
        Header.Raw(CIString("Cache-Control"), "no-cache")
      )
      def policy(req: Request[F], rep: Either[Throwable, Response[F]], n: Int): Option[FiniteDuration] =
        if (n < 10) Some(10.seconds) else None

      Retry(policy)(client)
      client.expect[AdobeTokenResponse](req)
    }
  }
}
