package mtest.http

import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.http.client.auth.{
  AuthorizationCode,
  ClientCredentials,
  Salesforce,
  UriJsonCodec
}
import io.circe.syntax.given
import io.circe.{Decoder, Encoder, Json}
import munit.FunSuite
import org.http4s.Uri
import org.http4s.implicits.*

class HttpCodecTest extends FunSuite {

  // ---------------------------------------------------------------------------
  // UriJsonCodec
  // ---------------------------------------------------------------------------

  import UriJsonCodec.given

  /** encode(a) decodes back to a, round-tripping through the JSON AST (no circe-parser dependency).
    * `assert(== Right(a))` avoids munit's cross-type Compare requirement on the DecodingFailure left side.
    */
  private def assertRoundTrip[A: Encoder: Decoder](a: A): Unit = {
    val json = a.asJson
    assert(json.as[A] == Right(a), s"round-trip failed for: ${json.noSpaces}")
  }

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
  // OAuth2 / Salesforce credential codecs (derives Codec.AsObject, snake_case keys,
  // Uri field routed through UriJsonCodec). These are wire-format sensitive.
  // ---------------------------------------------------------------------------

  test("9.ClientCredentials - round-trips without scope") {
    val cc = ClientCredentials(uri"https://auth.example.com/token", "id", "secret")
    assertRoundTrip(cc)
  }

  test("10.ClientCredentials - round-trips with scope") {
    val cc = ClientCredentials(
      uri"https://auth.example.com/token",
      "id",
      "secret",
      scope = Some(NonEmptyList.of("read", "write")))
    assertRoundTrip(cc)
  }

  test("11.ClientCredentials - wire keys are snake_case and uri renders as a string") {
    val cc = ClientCredentials(uri"https://auth.example.com/token", "id", "secret")
    val obj = cc.asJson.hcursor
    assertEquals(obj.get[String]("auth_endpoint"), Right("https://auth.example.com/token"))
    assertEquals(obj.get[String]("client_id"), Right("id"))
    assertEquals(obj.get[String]("client_secret"), Right("secret"))
  }

  test("12.AuthorizationCode - round-trips without scope") {
    val ac = AuthorizationCode(
      auth_endpoint = uri"https://auth.example.com/token",
      client_id = "id",
      client_secret = "secret",
      code = "auth-code",
      redirect_uri = "https://example.com/callback"
    )
    assertRoundTrip(ac)
  }

  test("13.AuthorizationCode - round-trips with scope") {
    val ac = AuthorizationCode(
      auth_endpoint = uri"https://auth.example.com/token",
      client_id = "id",
      client_secret = "secret",
      code = "auth-code",
      redirect_uri = "https://example.com/callback",
      scope = Some(NonEmptyList.one("openid"))
    )
    assertRoundTrip(ac)
  }

  test("14.Salesforce.PasswordGrant - round-trips") {
    val pg = Salesforce.PasswordGrant(
      auth_endpoint = uri"https://login.salesforce.com/services/oauth2/token",
      client_id = "id",
      client_secret = "secret",
      username = "user@example.com",
      password = "pw"
    )
    assertRoundTrip(pg)
  }

  test("15.Salesforce.PasswordGrant - wire keys are snake_case") {
    val pg = Salesforce.PasswordGrant(
      auth_endpoint = uri"https://login.salesforce.com/services/oauth2/token",
      client_id = "id",
      client_secret = "secret",
      username = "user@example.com",
      password = "pw"
    )
    val obj = pg.asJson.hcursor
    assertEquals(obj.get[String]("client_id"), Right("id"))
    assertEquals(obj.get[String]("client_secret"), Right("secret"))
    assertEquals(obj.get[String]("username"), Right("user@example.com"))
    assertEquals(obj.get[String]("password"), Right("pw"))
  }
}
