package com.github.chenharryhua.nanjin.http.aep.auth
import cats.effect.Concurrent
import com.github.chenharryhua.nanjin.http.auth.privateKey
import io.circe.generic.JsonCodec
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.http4s.Method.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Header, Uri}
import org.typelevel.ci.CIString

import java.io.File
import java.lang.Boolean.TRUE
import java.security.interfaces.RSAPrivateKey
import scala.collection.JavaConverters.*

@JsonCodec
final case class AepTokenResponse(token_type: String, expires_in: Long, access_token: String)

sealed abstract class AepTokenType(name: String)

object AepTokenType {

  final case class IMS(endpoint: String, client_id: String, client_code: String, client_secret: String)
      extends AepTokenType("access_token")

  final case class JWT[F[_]](
    auth_endpoint: String,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    key_path: String)
      extends AepTokenType("jwt_token") with Http4sClientDsl[F] {
    private val JWT_EXPIRY_KEY: String         = "exp"
    private val JWT_ISS_KEY: String            = "iss"
    private val JWT_AUD_KEY: String            = "aud"
    private val JWT_SUB_KEY: String            = "sub"
    private val JWT_TOKEN_EXPIRATION_THRESHOLD = 86400L // 24 hours

    def login(client: Client[F])(implicit F: Concurrent[F]): F[AepTokenResponse] = {
      val claims: Map[String, AnyRef] = Map(
        JWT_ISS_KEY -> ims_org_id,
        JWT_SUB_KEY -> technical_account_key,
        JWT_EXPIRY_KEY -> new java.lang.Long(System.currentTimeMillis() / 1000 + JWT_TOKEN_EXPIRATION_THRESHOLD),
        JWT_AUD_KEY -> s"$auth_endpoint/c/$client_id",
        s"$auth_endpoint/s/ent_dataservices_sdk" -> TRUE
      )
      val pk: RSAPrivateKey = privateKey.pkcs8File(new File(key_path))
      val jwtToken          = Jwts.builder.setClaims(claims.asJava).signWith(SignatureAlgorithm.RS256, pk).compact

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
      client.expect[AepTokenResponse](req)
    }
  }
}
