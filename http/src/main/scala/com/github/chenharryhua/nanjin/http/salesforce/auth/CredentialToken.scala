package com.github.chenharryhua.nanjin.http.salesforce.auth

import cats.{Applicative, MonadError, Show}
import io.circe.Decoder
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.io.*
import org.http4s.{Request, Uri, UrlForm}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Tokens which carry instance-url with it
  */
sealed trait TokenHasInstanceUrl[F[_], A] {
  def instanceUrl(a: A): F[Uri] // salesforce instance url which, combine with path segments, can build full path
}

/** Token which carry time-to-live info and an auth-string
  */
sealed trait TokenProperties[A] {
  def ttl(a: A): FiniteDuration // time to live
  def authStr(a: A): String //retrieve authentication toke from A
}

/** Credential which can create login form
  */
sealed trait CredLoginRequest[F[_], A] {
  def loginRequest(a: A): Request[F]
}

//https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
object iot {

  final case class Token(
    access_token: String,
    instance_url: String,
    id: String,
    token_type: String,
    issued_at: String,
    signature: String) {
    val authStr: String = s"$token_type $access_token"
  }

  object Token {
    implicit val showToken: Show[Token] = cats.derived.semiauto.show[Token]

    implicit def salesforceHasInstanceUrl[F[_]](implicit F: MonadError[F, Throwable]): TokenHasInstanceUrl[F, Token] =
      new TokenHasInstanceUrl[F, Token] {

        override def instanceUrl(a: Token): F[Uri] = Uri.fromString(a.instance_url) match {
          case Left(value)  => F.raiseError(new Error(s"fatal spec violation: $value"))
          case Right(value) => F.pure(value)
        }
      }

    implicit val salesforceTokenProperties: TokenProperties[Token] =
      new TokenProperties[Token] {
        override def ttl(a: Token): FiniteDuration = 1.hours //token refresh every 1 hour
        override def authStr(a: Token): String     = a.authStr
      }

    implicit val sfJsonTokenDecoder: Decoder[Token] =
      io.circe.generic.semiauto.deriveDecoder[Token]
  }

  final case class Credential[F[_]: Applicative](
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    authUri: Uri
  ) extends Http4sClientDsl[F] {

    val loginRequest: Request[F] = POST(
      UrlForm(
        "grant_type" -> "password",
        "client_id" -> client_id,
        "client_secret" -> client_secret,
        "username" -> username,
        "password" -> password
      ),
      authUri
    )
  }

  object Credential {

    implicit def salesforceLoginRequest[F[_]]: CredLoginRequest[F, Credential[F]] =
      new CredLoginRequest[F, Credential[F]] {
        override def loginRequest(a: Credential[F]): Request[F] = a.loginRequest
      }
  }
}

//https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm
object marketingCloud {

  final case class Token(
    access_token: String,
    token_type: String,
    expires_in: Long,
    scope: String,
    soap_instance_url: String,
    rest_instance_url: String) {
    val authStr: String     = s"$token_type $access_token"
    val ttl: FiniteDuration = FiniteDuration(expires_in / 2, TimeUnit.SECONDS)
  }

  object Token {
    implicit val showSalesforceMarketingCloudToken: Show[Token] = cats.derived.semiauto.show[Token]

    implicit def salesforceMarketingCloudHasInstanceUrl[F[_]](implicit
      F: MonadError[F, Throwable]): TokenHasInstanceUrl[F, Token] =
      new TokenHasInstanceUrl[F, Token] {

        override def instanceUrl(a: Token): F[Uri] = Uri.fromString(a.rest_instance_url) match {
          case Left(value)  => F.raiseError(new Error(s"fatal spec violation: $value"))
          case Right(value) => F.pure(value)
        }
      }

    implicit val salesforceMarketingCloudTokenProperties: TokenProperties[Token] =
      new TokenProperties[Token] {
        override def ttl(a: Token): FiniteDuration = a.ttl
        override def authStr(a: Token): String     = a.authStr
      }

    implicit val sfJsonTokenDecoder: Decoder[Token] =
      io.circe.generic.semiauto.deriveDecoder[Token]
  }

  final case class Credential[F[_]: Applicative](
    client_id: String,
    client_secret: String,
    authUri: Uri
  ) extends Http4sClientDsl[F] {

    val loginRequest: Request[F] = POST(
      UrlForm(
        "grant_type" -> "client_credentials",
        "client_id" -> client_id,
        "client_secret" -> client_secret
      ),
      authUri
    )
  }

  object Credential {

    implicit def salesforceMarketingCloudLoginRequest[F[_]]: CredLoginRequest[F, Credential[F]] =
      new CredLoginRequest[F, Credential[F]] {
        override def loginRequest(a: Credential[F]): Request[F] = a.loginRequest
      }
  }
}
