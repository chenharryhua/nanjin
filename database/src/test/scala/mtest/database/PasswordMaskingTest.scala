package mtest.database

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.database.*
import org.scalatest.funsuite.AnyFunSuite

class PasswordMaskingTest extends AnyFunSuite {

  private val secret = "s3cr3t-p@ss"

  test("1.Password.toString masks the value") {
    assert(Password(secret).toString == "***")
    assert(!Password(secret).toString.contains(secret))
  }

  test("2.Password.show masks the value") {
    import Password.given
    assert(Password(secret).show == "***")
  }

  test("3.value returns the real secret for use at the JDBC boundary") {
    assert(Password(secret).value == secret)
  }

  test("4.enclosing case class toString does not leak the password") {
    val pg = Postgres("user", Password(secret), "host", 5432, "db")
    val rendered = pg.toString
    assert(!rendered.contains(secret), s"password leaked in: $rendered")
    assert(rendered.contains("***"))
    // non-secret fields are still visible
    assert(rendered.contains("user"))
    assert(rendered.contains("host"))
  }

  test("5.Redshift and SqlServer toString also mask") {
    assert(!Redshift("u", Password(secret), "h", 1, "d").toString.contains(secret))
    assert(!SqlServer("u", Password(secret), "h", 1, "d").toString.contains(secret))
  }

  test("6.Password equality is by value") {
    assert(Password(secret) == Password(secret))
    assert(Password(secret) != Password("other"))
  }
}
