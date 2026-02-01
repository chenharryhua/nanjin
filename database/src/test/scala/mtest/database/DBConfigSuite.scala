package mtest.database

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import eu.timepit.refined.auto.*
import munit.CatsEffectSuite

class DBConfigSuite extends CatsEffectSuite {

  // Dummy Postgres credentials for testing
  val testDb = Postgres(
    host = "localhost",
    port = 5432,
    database = "postgres",
    username = "postgres",
    password = "postgres"
  )

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
      Postgres(Username("unknown"), Password("unknown"), Host("localhost"), 5432, DatabaseName("postgres"))
    val dbConfig = DBConfig(invalidDb)
    dbConfig.testConnection[IO].map { result =>
      assertEquals(result, false) // should fail to connect
    }
  }

}
