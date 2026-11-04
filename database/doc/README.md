# Database connection utilities

The database module builds doobie `HikariTransactor` resources from immutable PostgreSQL, Amazon
Redshift, and SQL Server connection descriptors. It configures the JDBC driver, URL, username, and
password, then lets the application apply additional HikariCP settings before creating a pool.

## Contents

- [Dependency and JDBC drivers](#dependency-and-jdbc-drivers)
- [Supported databases](#supported-databases)
- [Build a configuration](#build-a-configuration)
  - [PostgreSQL](#postgresql)
  - [Redshift](#redshift)
  - [SQL Server](#sql-server)
  - [HikariCP customization](#hikaricp-customization)
- [Use a transactor](#use-a-transactor)
  - [Resource form](#resource-form)
  - [Stream form](#stream-form)
  - [Statement logging](#statement-logging)
- [Test connectivity](#test-connectivity)
- [Protocols helper](#protocols-helper)
- [Security and operational guidance](#security-and-operational-guidance)
- [External configuration contract](#external-configuration-contract)
- [Source map and validation](#source-map-and-validation)

## Dependency and JDBC drivers

Add the module and the JDBC driver for each database used by the application:

```scala
libraryDependencies ++= Seq(
  "com.github.chenharryhua" %% "nj-database" % "<version>",

  // Choose the required driver or drivers:
  "org.postgresql" % "postgresql" % "<version>",
  "com.amazon.redshift" % "redshift-jdbc42" % "<version>",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "<version>"
)
```

`nj-database` includes doobie and HikariCP but does not publish the database-specific JDBC drivers as
runtime dependencies. `DBConfig` names a concrete driver class, so that driver must be present when
the descriptor is constructed. `HikariConfig.setDriverClassName` loads the class immediately; a
missing driver fails before a connection pool is acquired.

## Supported databases

| Descriptor | Driver class | Generated JDBC URL |
| --- | --- | --- |
| `Postgres` | `org.postgresql.Driver` | `jdbc:postgresql://<host>:<port>/<database>` |
| `Redshift` | `com.amazon.redshift.jdbc42.Driver` | `jdbc:redshift://<host>:<port>/<database>` |
| `SqlServer` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `jdbc:sqlserver://<host>:<port>;databaseName=<database>` |

Each descriptor contains `username`, `password`, `host`, `port`, and `database`. The password uses
Nanjin's `Secret`, so descriptor `toString` and Cats `Show` render it as `***`.

`DBConfig` directly supports only these three JDBC databases. `Protocols` also contains MongoDB and
Neo4j URL prefixes, but no MongoDB or Neo4j pool/transactor constructor is provided.

## Build a configuration

### PostgreSQL

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.database.{DBConfig, Postgres}

val postgres = Postgres(
  username = "orders_app",
  password = Secret(sys.env("POSTGRES_PASSWORD")),
  host = "postgres.example.com",
  port = 5432,
  database = "orders"
)

val postgresConfig = DBConfig(postgres)
```

The generated URL is:

```text
jdbc:postgresql://postgres.example.com:5432/orders
```

### Redshift

```scala
import com.github.chenharryhua.nanjin.database.Redshift

val redshift = Redshift(
  username = "analytics_app",
  password = Secret(sys.env("REDSHIFT_PASSWORD")),
  host = "warehouse.example.com",
  port = 5439,
  database = "analytics"
)

val redshiftConfig = DBConfig(redshift)
```

The Redshift constructor adds these Hikari data-source properties:

```text
ssl=true
sslfactory=com.amazon.redshift.ssl.NonValidatingFactory
```

`NonValidatingFactory` enables encryption without validating the server certificate. That default is
not sufficient when the client must authenticate the database endpoint. Override the SSL factory and
supply the trust configuration required by the deployment before using this configuration in a
production trust boundary.

### SQL Server

```scala
import com.github.chenharryhua.nanjin.database.SqlServer

val sqlServer = SqlServer(
  username = "reporting_app",
  password = Secret(sys.env("SQLSERVER_PASSWORD")),
  host = "sqlserver.example.com",
  port = 1433,
  database = "reporting"
)

val sqlServerConfig = DBConfig(sqlServer)
```

The database is encoded as the `databaseName` JDBC property in the generated URL.

### HikariCP customization

`DBConfig.set` records a `HikariConfig => Unit` mutation and returns a new `DBConfig`:

```scala
import scala.concurrent.duration.*

val configured =
  DBConfig(postgres)
    .set(_.setPoolName("orders-pool"))
    .set(_.setMaximumPoolSize(16))
    .set(_.setMinimumIdle(2))
    .set(_.setConnectionTimeout(10.seconds.toMillis))
    .set(_.setValidationTimeout(3.seconds.toMillis))
    .set(_.setIdleTimeout(5.minutes.toMillis))
```

The functions are not executed when `set` is called. `hikariConfig` copies the initial configuration,
replays mutations in insertion order, validates the result, caches it, and returns that effective
`HikariConfig`. Later mutations for the same property win:

```scala
val pools =
  DBConfig(postgres)
    .set(_.setMaximumPoolSize(4))
    .set(_.setMaximumPoolSize(12))

pools.hikariConfig.getMaximumPoolSize // 12
```

The original value remains unchanged when another configuration is derived from it. Validation can
throw for invalid Hikari settings. Avoid effectful or nondeterministic work inside `set`; each
configuration expects plain mutations of its fresh Hikari copy.

Database-specific properties can also be set through Hikari:

```scala
val verifiedRedshift =
  DBConfig(redshift)
    .set(
      _.addDataSourceProperty(
        "sslfactory",
        "<certificate-validating-redshift-ssl-factory>"
      )
    )
```

Use the SSL factory supported by the selected Redshift driver and deployment rather than copying the
placeholder.

## Use a transactor

### Resource form

`transactorR` creates the Hikari pool when acquired and closes it when released:

```scala
import cats.syntax.all.*
import org.typelevel.doobie.ConnectionIO
import org.typelevel.doobie.syntax.string.toSqlInterpolator

val selectOrder: ConnectionIO[Option[String]] =
  sql"select status from orders where id = ${123L}"
    .query[String]
    .option

val status: IO[Option[String]] =
  configured
    .transactorR[IO](logHandler = None)
    .use(transactor => transactor.trans.apply(selectOrder))
```

Keep the transactor resource open for the application lifetime. Acquiring it for every statement
creates and closes a complete connection pool for every statement.

A transaction is one `ConnectionIO` program interpreted once through the transactor:

```scala
val markComplete: ConnectionIO[Int] =
  sql"update orders set status = 'complete' where id = ${123L}".update.run

val program: ConnectionIO[(Option[String], Int)] =
  (selectOrder, markComplete).tupled

val transaction: IO[(Option[String], Int)] =
  configured.transactorR[IO](None).use { transactor =>
    transactor.trans.apply(program)
  }
```

### Stream form

`transactorS` exposes the same managed transactor as a single-element fs2 stream:

```scala
import fs2.Stream

val statusStream: Stream[IO, Option[String]] =
  configured
    .transactorS[IO](None)
    .evalMap(transactor => transactor.trans.apply(selectOrder))
```

Use the stream form when the pool belongs naturally in a larger fs2 resource graph. Both forms have
the same Hikari acquisition and shutdown behavior.

### Statement logging

Both constructors take `Option[LogHandler[F]]`. Pass `None` to disable doobie statement-event
logging or a configured handler to observe execution, arguments, timing, and failures.

SQL log events can contain statement text and encoded parameter values. Redact sensitive values and
apply an appropriate log threshold before enabling a handler in production.

## Test connectivity

`testConnection[F]` creates a temporary transactor, executes this query, and closes the pool:

```sql
select 42
```

```scala
val reachable: IO[Boolean] = configured.testConnection[IO]
```

It returns `true` when pool acquisition, the scalar query, and pool release succeed. Errors during
those resource operations become `false`, but configuration construction or validation can throw
synchronously before an `F[Boolean]` is returned. Use an ordinary transactor operation when the caller
needs the failure cause. Because each call creates a new pool, `testConnection` is suitable for startup
checks and diagnostics, not a high-frequency health endpoint.

The query is intentionally simple but still assumes that the target database accepts `select 42` and
that the configured user can execute it.

## Protocols helper

`Protocols.url` combines a protocol prefix, host, and optional port:

```scala
import com.github.chenharryhua.nanjin.database.Protocols

Protocols.Postgres.url("db.example.com", Some(5432))
// jdbc:postgresql://db.example.com:5432

Protocols.MongoDB.url("mongo.example.com", None)
// mongodb://mongo.example.com
```

Available prefixes:

| Case | Prefix |
| --- | --- |
| `MongoDB` | `mongodb` |
| `Postgres` | `jdbc:postgresql` |
| `Redshift` | `jdbc:redshift` |
| `SqlServer` | `jdbc:sqlserver` |
| `Neo4j` | `bolt` |

The helper performs string composition only. It does not escape host names, database names, query
parameters, or credentials and does not establish a connection.

## Security and operational guidance

- Load passwords from a secret manager or runtime environment. Do not commit them to source control.
- `Secret` masks descriptor rendering, but `DBConfig` must place the raw password in `HikariConfig`.
  Never log or serialize the effective Hikari configuration.
- Replace Redshift's `NonValidatingFactory` when certificate validation is required.
- Use TLS settings appropriate to PostgreSQL, Redshift, and SQL Server. A JDBC URL alone does not
  establish the required trust policy.
- Grant the database user only the schema and statement permissions the service needs.
- Size the pool against application concurrency and the database's connection limit. More application
  fibers do not justify unbounded database connections.
- Set connection, validation, idle, lifetime, and leak-detection thresholds deliberately.
- Hold `transactorR` or `transactorS` for the intended service lifetime so Hikari closes connections
  on release.
- Keep one logical transaction inside one `ConnectionIO` program and interpret it once through the
  transactor.
- Review doobie `LogHandler` output before enabling SQL argument logging.
- Do not use `testConnection` as the only production health signal; it hides the underlying error.

## External configuration contract

The module defines no JSON wire format for database descriptors. The following values are still
externally observed configuration and should change only with a deployment migration:

- JDBC driver class names.
- JDBC URL prefixes and database-specific URL layout.
- Hosts, ports, database names, and usernames.
- HikariCP property names and pool settings.
- Redshift data-source keys `ssl` and `sslfactory`.
- SQL text and database object names used by the application.

Descriptor type or Scala identifier renames do not alter the database protocol by themselves. A
change to URL construction, driver class, data-source properties, or SQL does.

## Source map and validation

| Area | Source |
| --- | --- |
| Hikari and doobie configuration | [`DBConfig.scala`](../src/main/scala/com/github/chenharryhua/nanjin/database/DBConfig.scala) |
| Database descriptors | [`data.scala`](../src/main/scala/com/github/chenharryhua/nanjin/database/data.scala) |
| Protocol URL helper | [`Protocols.scala`](../src/main/scala/com/github/chenharryhua/nanjin/database/Protocols.scala) |

Offline compilation:

```shell
sbt 'database / Test / compile'
```

Pure configuration and masking tests do not open database connections:

```shell
sbt "database/testOnly mtest.database.PostgresConfigTest mtest.database.RedshiftSqlServerConfigTest mtest.database.SecretMaskingTest"
```

The full test suite also includes `DBConfigSuite` and `DoobieMetaTest`, which expect PostgreSQL at
`localhost:5432` with database, username, and password all set to `postgres`:

```shell
sbt 'database / test'
```
