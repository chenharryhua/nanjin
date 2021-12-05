package mtest

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.datetime.sydneyTime
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

package object spark {
  val akkaSystem: ActorSystem    = ActorSystem("nj-spark")
  implicit val mat: Materializer = Materializer(akkaSystem)

  val sparkSession: SparkSession = SparkSettings(sydneyTime)
    .withAppName("nj.spark.test")
    .withLogLevel(NJLogLevel.INFO)
    .withoutUI
    .withUI
    .unsafeSession
}
