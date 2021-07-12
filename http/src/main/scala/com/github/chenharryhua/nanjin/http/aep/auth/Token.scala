package com.github.chenharryhua.nanjin.http.aep.auth
import io.circe.generic.JsonCodec
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}

import java.time.Instant

@JsonCodec
final case class TokenResponse(token_type: String, expires_in: Long, refresh_token: String, access_token: String)

@JsonCodec
sealed abstract class TokenType(name: String)

object TokenType {
  final case class IMS(endpoint: String, client_id: String, client_code: String, client_secret: String)
      extends TokenType("access_token") {
    val IMS_ENDPOINT_PATH = "/ims/token/v1"
  }
  final case class JWT(
    endpoint: String,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    key_path: String)
      extends TokenType("jwt_token") {

    val claim = JwtClaim(
      expiration = Some(Instant.now.plusSeconds(157784760).getEpochSecond),
      issuedAt = Some(Instant.now.getEpochSecond)
    )
    val key  = "secretKey"
    val algo = JwtAlgorithm.HS256

    val token = JwtCirce.encode(claim, key, algo)

    JwtCirce.decodeJson(token, key, Seq(JwtAlgorithm.HS256))
    JwtCirce.decode(token, key, Seq(JwtAlgorithm.HS256))
  }
}
