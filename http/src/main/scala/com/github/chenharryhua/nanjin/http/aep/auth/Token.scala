package com.github.chenharryhua.nanjin.http.aep.auth
import com.github.chenharryhua.nanjin.http.auth.privateKey
import io.circe.generic.JsonCodec
import org.http4s.Method.*
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, UrlForm}
import pdi.jwt.{JwtAlgorithm, JwtClaim, JwtUtils}

import java.io.File
import java.time.Clock

@JsonCodec
final case class TokenResponse(token_type: String, expires_in: Long, refresh_token: String, access_token: String)

sealed abstract class TokenType(name: String)

object TokenType {
  final case class IMS(endpoint: String, client_id: String, client_code: String, client_secret: String)
      extends TokenType("access_token")

  final case class JWT[F[_]](
    endpoint: String,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    key_path: String)
      extends TokenType("jwt_token") with Http4sClientDsl[F] {

    def login: Request[F] = {
      val claim = JwtClaim(
        issuer = Some(ims_org_id),
        subject = Some(technical_account_key),
        audience = Some(Set(s"$endpoint/c/$client_id")))
        .expiresIn(60 * 60)(Clock.systemUTC()) + (s"$endpoint/s/ent_dataservices_sdk", true)
      val jwt =
        JwtUtils.stringify(JwtUtils.sign(claim.toJson, privateKey.pkcs8File(new File(key_path)), JwtAlgorithm.RS256))
      POST(
        UrlForm(
          "client_id" -> client_id,
          "client_secret" -> client_secret,
          "jwt_token" -> jwt
        ),
        uri"https://ims-na1.adobelogin.com/ims/exchange/jwt"
      )
    }
  }
}
