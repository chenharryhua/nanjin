package mtest.spark.dstream

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import fs2.kafka.ProducerRecord
import mtest.spark.kafka.sparKafka
import mtest.spark.sparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

class SparkDStreamTest extends AnyFunSuite {
  val root: String = "./data/test/spark/dstream/"
  val checkpoint   = "./data/test/spark/dstream/checkpont/"

  val runner: DStreamRunner[IO] = DStreamRunner[IO](sparkSession.sparkContext, checkpoint, 1.second)

  val topic: SparKafkaTopic[IO, Int, String] = sparKafka.topic[Int, String]("dstream.test")

}
