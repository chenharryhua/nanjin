package com.github.chenharryhua.nanjin.salesforce.auth

import org.http4s.Uri

final case class MarketingCloudCred(client_id: String, client_secret: String, authUri: Uri)

final case class MarketingCloudToken(
  access_token: String,
  token_type: String,
  expires_in: Long,
  scope: String,
  soap_instance_url: String,
  rest_instance_url: String)

final case class SalesforceCred(
  client_id: String,
  client_secret: String,
  username: String,
  password: String,
  authUri: Uri)

final case class SalesforceToken(
  access_token: String,
  instance_url: String,
  id: String,
  token_type: String,
  issued_at: String,
  signature: String)
