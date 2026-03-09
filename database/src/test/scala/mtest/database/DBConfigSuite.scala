package mtest.database

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import munit.CatsEffectSuite

import scala.language.implicitConversions

val testDb: Postgres = Postgres(
  host = Host.unsafeFrom("localhost"),
  port = Port.unsafeFrom(5432),
  database = DatabaseName.unsafeFrom("postgres"),
  username = Username.unsafeFrom("postgres"),
  password = Password.unsafeFrom("postgres")
)

class DBConfigSuite extends CatsEffectSuite {

  // Dummy Postgres credentials for testing

  test("DBConfig.apply(Postgres) creates valid HikariConfig") {
    val dbConfig = DBConfig(testDb)
      .set(_.setMaximumPoolSize(5)) // test set DSL
      .set(_.setConnectionTimeout(5000))

    val hikari = dbConfig.hikariConfig

    assertEquals(hikari.getDriverClassName, "org.postgresql.Driver")
    assert(hikari.getJdbcUrl.contains("postgres"))
    assertEquals(hikari.getUsername, "postgres")
    assertEquals(hikari.getMaximumPoolSize, 5)
    assertEquals(hikari.getConnectionTimeout, 5000L)
  }

  test("DBConfig.transactorR creates HikariTransactor") {
    val dbConfig = DBConfig(testDb)
    dbConfig.transactorR[IO](None).use { xa =>
      IO {
        assertEquals(xa.kernel.getClass.getSimpleName.contains("HikariDataSource"), true)
      }
    }
  }

  test("DBConfig.testConnection returns false on invalid DB") {
    val invalidDb =
      Postgres(
        Username.unsafeFrom("unknown"),
        Password.unsafeFrom("unknown"),
        Host.unsafeFrom("localhost"),
        Port.unsafeFrom(5432),
        DatabaseName.unsafeFrom("postgres")
      )
    val dbConfig = DBConfig(invalidDb)
    dbConfig.testConnection[IO].map { result =>
      assertEquals(result, false) // should fail to connect
    }
  }

}
