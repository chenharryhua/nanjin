package mtest.spark

import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.database.{
  DatabaseName,
  Host,
  Password,
  Port,
  Postgres,
  Username
}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

package object database {

  implicit val sparkSession: SparkSession =
    SparkSettings.default
      .withConfigUpdate(_.setMaster("local[*]").setAppName("test-spark"))
      .withLogLevel(NJLogLevel.ERROR)
      .session

  val db: Postgres = Postgres(
    Username("postgres"),
    Password("postgres"),
    Host("localhost"),
    Port(5432),
    DatabaseName("postgres"))
}
