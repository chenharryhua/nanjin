package mtest.database

import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.database.*
import munit.FunSuite

/** Pure tests for the Postgres wiring: the `Protocols` JDBC prefix/URL builder, and how `DBConfig(Postgres)`
  * composes the driver, JDBC URL, credentials, and replays `set` mutations. Complements `DBConfigSuite`
  * (which covers transactor/connection) and `SecretMaskingTest` (password masking) without duplicating them.
  */
class PostgresConfigTest extends FunSuite {

  private def pg(host: String = "localhost", port: Int = 5432, database: String = "mydb"): Postgres =
    Postgres("user", Secret("pw"), host, port, database)

  // ---- Protocols ----------------------------------------------------------------------------------

  test("1.Protocols values are the JDBC/driver prefixes") {
    assert(Protocols.Postgres.value == "jdbc:postgresql")
    assert(Protocols.Redshift.value == "jdbc:redshift")
    assert(Protocols.SqlServer.value == "jdbc:sqlserver")
    assert(Protocols.MongoDB.value == "mongodb")
    assert(Protocols.Neo4j.value == "bolt")
  }

  test("2.Protocols.url includes the port when provided") {
    assert(Protocols.Postgres.url("h", Some(5432)) == "jdbc:postgresql://h:5432")
  }

  test("3.Protocols.url omits the port when absent") {
    assert(Protocols.Postgres.url("h", None) == "jdbc:postgresql://h")
  }

  // ---- DBConfig(Postgres) -------------------------------------------------------------------------

  test("4.DBConfig(Postgres) sets the postgres driver") {
    assert(DBConfig(pg()).hikariConfig.getDriverClassName == "org.postgresql.Driver")
  }

  test("5.DBConfig(Postgres) composes host, port, and database into the JDBC URL") {
    val hikari = DBConfig(pg(host = "db.example.com", port = 6543, database = "orders")).hikariConfig
    assert(hikari.getJdbcUrl == "jdbc:postgresql://db.example.com:6543/orders")
  }

  test("6.DBConfig(Postgres) propagates username and the raw secret password") {
    val hikari = DBConfig(Postgres("alice", Secret("s3cr3t"), "h", 5432, "d")).hikariConfig
    assert(hikari.getUsername == "alice")
    assert(hikari.getPassword == "s3cr3t") // the raw value crosses the JDBC boundary
  }

  // ---- set DSL: recorded, replayed in insertion order -----------------------------------------------

  test("7.set records a mutation applied when hikariConfig is built") {
    val hikari = DBConfig(pg()).set(_.setMaximumPoolSize(7)).hikariConfig
    assert(hikari.getMaximumPoolSize == 7)
  }

  test("8.set is immutable: the original DBConfig is unaffected") {
    val base = DBConfig(pg()).set(_.setMaximumPoolSize(3))
    val bumped = base.set(_.setMaximumPoolSize(9))
    assert(base.hikariConfig.getMaximumPoolSize == 3)
    assert(bumped.hikariConfig.getMaximumPoolSize == 9)
  }

  test("9.set mutations replay in insertion order (last write wins for the same property)") {
    val hikari = DBConfig(pg())
      .set(_.setMaximumPoolSize(1))
      .set(_.setMaximumPoolSize(2))
      .set(_.setMaximumPoolSize(3))
      .hikariConfig
    assert(hikari.getMaximumPoolSize == 3)
  }
}
