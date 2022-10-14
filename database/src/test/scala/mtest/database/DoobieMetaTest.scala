package mtest.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.common.database.{DatabaseName, Host, Password, Port, Postgres, Username}
import com.github.chenharryhua.nanjin.database.NJHikari
import doobie.ConnectionIO
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val postgres: Postgres =
    Postgres(
      Username("unknown"),
      Password("unknown"),
      Host("localhost"),
      Port(5432),
      DatabaseName("postgres"))

  test("setter") {
    val username = Username("postgres")
    val password = Password("postgres")
    val nj = NJHikari(postgres)
      .set(_.setUsername(username.value))
      .set(_.setPassword(password.value))
      .set(_.setMaximumPoolSize(42))
    assert(nj.hikariConfig.getUsername == username.value)
    assert(nj.hikariConfig.getPassword == password.value)
    assert(nj.hikariConfig.getMaximumPoolSize == 42)

    nj.transactorResource[IO].use(_.trans.apply(2.pure[ConnectionIO])).unsafeRunSync()
  }
}
