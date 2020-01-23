import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.database.{Host, Password, Port, Username}
import com.github.chenharryhua.nanjin.graph.{MorpheusNeo4jSession, Neo4jSettings, NeotypesSession}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val sparkSession: SparkSession =
    SparkSettings.default.withConfigUpdate(_.setMaster("local[*]").setAppName("test-morpheus")).session

  val config                         = Neo4jSettings(Username("neo4j"), Password("test"), Host("localhost"), Port(7687))
  val morpheus: MorpheusNeo4jSession = config.morpheus(sparkSession)
  val ntSession: NeotypesSession[IO] = config.neotypes[IO]
}
