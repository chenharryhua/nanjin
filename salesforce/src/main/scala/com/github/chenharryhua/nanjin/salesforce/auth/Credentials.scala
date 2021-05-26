package com.github.chenharryhua.nanjin.salesforce.auth

import org.http4s.Uri
//https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm
final case class MarketingCloudLogin(client_id: String, client_secret: String, authUri: Uri)

final case class MarketingCloudToken(
  access_token: String,
  token_type: String,
  expires_in: Long,
  scope: String,
  soap_instance_url: String,
  rest_instance_url: String)

//https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
final case class IotLogin(client_id: String, client_secret: String, username: String, password: String, authUri: Uri)

final case class IotToken(
  access_token: String,
  instance_url: String,
  id: String,
  token_type: String,
  issued_at: String,
  signature: String)
