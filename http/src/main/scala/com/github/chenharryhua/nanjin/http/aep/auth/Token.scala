package com.github.chenharryhua.nanjin.http.aep.auth
import io.circe.generic.JsonCodec

@JsonCodec
final case class TokenResponse(token_type: String, expires_in: Long, refresh_token: String, access_token: String)

@JsonCodec
sealed abstract class TokenType(name: String)

object TokenType {
  final case class IMS(endpoint: String, client_idd: String, client_code: String, client_secret: String)
      extends TokenType("access_token")
  final case class JWT(
    endpoint: String,
    ims_org_id: String,
    client_id: String,
    client_secret: String,
    technical_account_key: String,
    key_path: String)
      extends TokenType("jwt_token")
}

object constant {
  val AUTH_ENDPOINT: String              = "auth.endpoint.id"
  val AUTH_CLIENT_ID: String             = "auth.client.id"
  val AUTH_CLIENT_CODE: String           = "auth.client.code"
  val AUTH_CLIENT_SECRET: String         = "auth.client.secret"
  val AUTH_IMS_ORG_ID: String            = "auth.client.imsOrgId"
  val AUTH_TECHNICAL_ACCOUNT_ID: String  = "auth.client.technicalAccountKey"
  val AUTH_META_SCOPE: String            = "auth.client.metaScope"
  val AUTH_PRIVATE_KEY_FILE_PATH: String = "auth.client.filePath"

  val TOKEN_EXPIRATION_THRESHOLD: Long     = 30000
  val DEFAULT_TOKEN_UPDATE_THRESHOLD: Long = 60000
}
