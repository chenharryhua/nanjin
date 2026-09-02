package mtest.common

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.Secret
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
}
