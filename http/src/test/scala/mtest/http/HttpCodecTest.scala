package mtest.http

import cats.data.NonEmptyList
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.http.client.auth.{
  AuthorizationCode,
  ClientCredentials,
  Password,
  Salesforce,
  UriJsonCodec
}
import io.circe.syntax.given
import io.circe.Json
import munit.FunSuite
import org.http4s.Uri
import org.http4s.implicits.*

class HttpCodecTest extends FunSuite {

  // ---------------------------------------------------------------------------
  // UriJsonCodec
  // ---------------------------------------------------------------------------

  import UriJsonCodec.given

  private def uriRoundTrip(uri: Uri, rendered: String): Unit = {
    val json = uri.asJson
    assertEquals(json.noSpaces, s"\"$rendered\"")
    assertEquals(json.as[Uri], Right(uri))
  }

  test("1.UriJsonCodec - round-trips a plain https uri") {
    uriRoundTrip(uri"https://example.com/api", "https://example.com/api")
  }

  test("2.UriJsonCodec - round-trips an explicit port") {
    uriRoundTrip(uri"https://example.com:8443/api", "https://example.com:8443/api")
  }

  test("3.UriJsonCodec - round-trips a query string") {
    uriRoundTrip(uri"https://example.com/api?a=1&b=2", "https://example.com/api?a=1&b=2")
  }

  test("4.UriJsonCodec - round-trips a fragment") {
    uriRoundTrip(uri"https://example.com/api#section", "https://example.com/api#section")
  }

  test("5.UriJsonCodec - round-trips userinfo") {
    uriRoundTrip(uri"https://user:pass@example.com/api", "https://user:pass@example.com/api")
  }

  test("6.UriJsonCodec - round-trips a relative uri") {
    uriRoundTrip(uri"/token", "/token")
  }

  test("7.UriJsonCodec - decode fails on a non-string json") {
    assert(Json.fromInt(42).as[Uri].isLeft)
  }

  test("8.UriJsonCodec - decode of an invalid uri yields Left with a message") {
    // a space is not a valid uri character
    val result = Json.fromString("http://exa mple.com").as[Uri]
    assert(result.isLeft, s"expected Left, got $result")
    assert(result.left.exists(_.message.nonEmpty))
  }

  // ---------------------------------------------------------------------------
  // Password masking (security): secrets must never render in cleartext, and the
  // credential types must not derive a JSON codec that could serialize them.
  // ---------------------------------------------------------------------------

  private val secret = "s3cr3t-p@ss"

  test("9.Password.toString and show mask the value") {
    assert(Password(secret).toString == "***")
    assert(Password(secret).show == "***")
    assert(!Password(secret).toString.contains(secret))
  }

  test("10.Password.value returns the real secret") {
    assert(Password(secret).value == secret)
  }

  test("11.ClientCredentials.toString does not leak client_secret") {
    val cc = ClientCredentials(uri"https://auth.example.com/token", "id", Password(secret))
    val rendered = cc.toString
    assert(!rendered.contains(secret), s"secret leaked in: $rendered")
    assert(rendered.contains("***"))
    assert(rendered.contains("id")) // non-secret field still visible
  }

  test("12.AuthorizationCode.toString masks client_secret and code") {
    val ac = AuthorizationCode(
      auth_endpoint = uri"https://auth.example.com/token",
      client_id = "id",
      client_secret = Password(secret),
      code = Password("auth-code-secret"),
      redirect_uri = "https://example.com/callback",
      scope = Some(NonEmptyList.one("openid"))
    )
    val rendered = ac.toString
    assert(!rendered.contains(secret))
    assert(!rendered.contains("auth-code-secret"))
    assert(rendered.contains("***"))
  }

  test("13.Salesforce.PasswordGrant.toString masks client_secret and password") {
    val pg = Salesforce.PasswordGrant(
      auth_endpoint = uri"https://login.salesforce.com/services/oauth2/token",
      client_id = "id",
      client_secret = Password(secret),
      username = "user@example.com",
      password = Password("pw-secret")
    )
    val rendered = pg.toString
    assert(!rendered.contains(secret))
    assert(!rendered.contains("pw-secret"))
    assert(rendered.contains("***"))
    assert(rendered.contains("user@example.com")) // username is not a secret
  }
}
