package mtest.database

import com.github.chenharryhua.nanjin.common.database.{DatabaseName, Host, Password, Port, Postgres, Username}
import com.github.chenharryhua.nanjin.database.NJHikari
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import java.time.*

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val postgres: Postgres =
    Postgres(
      Username("postgres"),
      Password("postgres"),
      Host("localhost"),
      Port(5432),
      DatabaseName("postgres"))

  implicit val zoneId: ZoneId = ZoneId.systemDefault()

  test("setter") {
    val username = Username("sergtsop")
    val password = Password("password")
    val nj       = NJHikari(postgres).set(_.setUsername(username.value)).set(_.setPassword(password.value))
    assert(nj.hikariConfig.getUsername == username.value)
    assert(nj.hikariConfig.getPassword == password.value)

  }

}
