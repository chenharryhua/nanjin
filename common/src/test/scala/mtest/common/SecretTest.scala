package mtest.common

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import munit.FunSuite

class SecretTest extends FunSuite {

  private val secret = "s3cr3t-p@ss"

  test("1.toString masks the value") {
    assert(Secret(secret).toString == "***")
    assert(!Secret(secret).toString.contains(secret))
  }

  test("2.show masks the value") {
    assert(Secret(secret).show == "***")
  }

  test("3.value returns the real secret") {
    assert(Secret(secret).value == secret)
  }

  test("4.equality is by value") {
    assert(Secret(secret) == Secret(secret))
    assert(Secret(secret) != Secret("other"))
    assert(Secret(secret).hashCode == Secret(secret).hashCode)
  }

  test("5.equality holds for same-length and differing-length values") {
    // the constant-time compare still returns the right answer: equal, same-length-but-different, and
    // different-length secrets all classify correctly
    val a = Secret("abcdef")
    assert(a == Secret("abcdef"))
    assert(a != Secret("abcdeX")) // same length, last byte differs
    assert(a != Secret("Xbcdef")) // same length, first byte differs
    assert(a != Secret("abc")) // shorter
    assert(a != Secret("abcdefg")) // longer
    assert(Secret("") == Secret("")) // empty secrets are equal
    assert(Secret("") != Secret("a"))
  }

  test("6.equality preserves malformed UTF-16 code units") {
    val unpairedSurrogate = Secret("\uD800")
    assert(unpairedSurrogate == Secret("\uD800"))
    assert(unpairedSurrogate != Secret("?"))
    assert(Set(unpairedSurrogate, Secret("?")).size == 2)
  }

  test("7.json encoder masks the value") {
    val json = Secret(secret).asJson
    assert(json == io.circe.Json.fromString("***"))
    assert(!json.noSpaces.contains(secret))
  }

  test("8.enclosing type derives a codec that masks the secret field") {
    final case class DbConfig(user: String, password: Secret) derives Encoder.AsObject

    val json = DbConfig("alice", Secret(secret)).asJson
    assert(json.hcursor.get[String]("user").toOption.contains("alice"))
    assert(json.hcursor.get[String]("password").toOption.contains("***"))
    assert(!json.noSpaces.contains(secret))
  }
}
