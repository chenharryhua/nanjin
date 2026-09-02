package mtest.common

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class SecretTest extends AnyFunSuite {

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

  test("5.string literal converts to Secret at a call site") {
    val s: Secret = "literal-secret"
    assert(s.value == "literal-secret")
  }

  test("6.json encoder masks the value") {
    val json = Secret(secret).asJson
    assert(json == io.circe.Json.fromString("***"))
    assert(!json.noSpaces.contains(secret))
  }

  test("7.enclosing type derives a codec that masks the secret field") {
    final case class DbConfig(user: String, password: Secret) derives Encoder.AsObject

    val json = DbConfig("alice", Secret(secret)).asJson
    assert(json.hcursor.get[String]("user").toOption.contains("alice"))
    assert(json.hcursor.get[String]("password").toOption.contains("***"))
    assert(!json.noSpaces.contains(secret))
  }
}
