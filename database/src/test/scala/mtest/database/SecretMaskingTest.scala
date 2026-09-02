package mtest.database

import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.database.*
import org.scalatest.funsuite.AnyFunSuite

class SecretMaskingTest extends AnyFunSuite {

  private val secret = "s3cr3t-p@ss"

  test("1.Postgres.toString does not leak the password") {
    val pg = Postgres("user", Secret(secret), "host", 5432, "db")
    val rendered = pg.toString
    assert(!rendered.contains(secret), s"password leaked in: $rendered")
    assert(rendered.contains("***"))
    // non-secret fields are still visible
    assert(rendered.contains("user"))
    assert(rendered.contains("host"))
  }

  test("2.Redshift and SqlServer toString also mask the password") {
    assert(!Redshift("u", Secret(secret), "h", 1, "d").toString.contains(secret))
    assert(!SqlServer("u", Secret(secret), "h", 1, "d").toString.contains(secret))
  }

  test("3.password.value is available for the JDBC boundary") {
    val pg = Postgres("user", Secret(secret), "host", 5432, "db")
    assert(pg.password.value == secret)
  }
}
