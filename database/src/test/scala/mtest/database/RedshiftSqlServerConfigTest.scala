package mtest.database

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.database.{DBConfig, Redshift, SqlServer}
import munit.FunSuite

/** Pure tests for the Redshift and SQL Server `DBConfig` overloads.
  *
  * These complement `PostgresConfigTest`, which covers the corresponding PostgreSQL driver, URL, credentials,
  * and immutable `set` behavior. The JDBC drivers are test-scoped dependencies: constructing a `HikariConfig`
  * validates that its configured driver class can be loaded, but these tests never open a database
  * connection.
  */
class RedshiftSqlServerConfigTest extends FunSuite {

  private def redshift(
    host: String = "localhost",
    port: Int = 5439,
    database: String = "warehouse"): Redshift =
    Redshift("user", Secret("pw"), host, port, database)

  private def sql_server(
    host: String = "localhost",
    port: Int = 1433,
    database: String = "warehouse"): SqlServer =
    SqlServer("user", Secret("pw"), host, port, database)

  test("1.DBConfig(Redshift) sets the Redshift JDBC driver") {
    val hikari = DBConfig(redshift()).hikariConfig
    assertEquals(hikari.getDriverClassName, "com.amazon.redshift.jdbc42.Driver")
  }

  test("2.DBConfig(Redshift) composes host, port, and database into the JDBC URL") {
    val hikari = DBConfig(redshift("redshift.example.com", 15439, "sales-data")).hikariConfig
    assertEquals(hikari.getJdbcUrl, "jdbc:redshift://redshift.example.com:15439/sales-data")
  }

  test("3.DBConfig(Redshift) propagates credentials and required SSL properties") {
    val hikari =
      DBConfig(Redshift("alice", Secret("redshift-secret"), "host", 5439, "database")).hikariConfig

    assertEquals(hikari.getUsername, "alice")
    assertEquals(hikari.getPassword, "redshift-secret")
    assertEquals(hikari.getDataSourceProperties.getProperty("ssl"), "true")
    assertEquals(
      hikari.getDataSourceProperties.getProperty("sslfactory"),
      "com.amazon.redshift.ssl.NonValidatingFactory")
  }

  test("4.DBConfig(SqlServer) sets the SQL Server JDBC driver") {
    val hikari = DBConfig(sql_server()).hikariConfig
    assertEquals(hikari.getDriverClassName, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  }

  test("5.DBConfig(SqlServer) composes host, port, and database into the JDBC URL") {
    val hikari = DBConfig(sql_server("sql.example.com", 11433, "sales-data")).hikariConfig
    assertEquals(hikari.getJdbcUrl, "jdbc:sqlserver://sql.example.com:11433;databaseName=sales-data")
  }

  test("6.DBConfig(SqlServer) propagates username and the raw secret password") {
    val hikari =
      DBConfig(SqlServer("alice", Secret("sqlserver-secret"), "host", 1433, "database")).hikariConfig

    assertEquals(hikari.getUsername, "alice")
    assertEquals(hikari.getPassword, "sqlserver-secret")
  }

  test("7.Redshift and SqlServer Show mask passwords while retaining connection fields") {
    val redshift_rendered =
      Redshift("redshift-user", Secret("redshift-secret"), "redshift-host", 5439, "redshift-db").show
    val sql_server_rendered =
      SqlServer("sql-user", Secret("sql-secret"), "sql-host", 1433, "sql-db").show

    assert(!redshift_rendered.contains("redshift-secret"), s"password leaked in: $redshift_rendered")
    assert(redshift_rendered.contains("***"))
    assert(redshift_rendered.contains("redshift-user"))
    assert(redshift_rendered.contains("redshift-host"))
    assert(redshift_rendered.contains("redshift-db"))

    assert(!sql_server_rendered.contains("sql-secret"), s"password leaked in: $sql_server_rendered")
    assert(sql_server_rendered.contains("***"))
    assert(sql_server_rendered.contains("sql-user"))
    assert(sql_server_rendered.contains("sql-host"))
    assert(sql_server_rendered.contains("sql-db"))
  }
}
