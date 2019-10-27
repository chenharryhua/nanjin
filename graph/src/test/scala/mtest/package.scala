import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.database._
import com.github.chenharryhua.nanjin.graph.{Neo4j, Neo4jSession}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val sparkSession: SparkSession =
    SparkSettings.default.updateConf(_.setMaster("local[*]").setAppName("test-morpheus")).session

  val neo4jConfig = Neo4j(Username("neo4j"), Password("test"), Host("localhost"), Port(7687))
  val neo4j       = Neo4jSession(neo4jConfig, sparkSession)
}
