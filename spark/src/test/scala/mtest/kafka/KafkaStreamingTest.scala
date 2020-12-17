package mtest.kafka

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import mtest.spark._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

object KafkaStreamingCases {

  case class StreamOne(name: String, size: Int)

  case class TableTwo(name: String, color: Int)

  case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

  val s1Data: RDD[NJProducerRecord[Int, StreamOne]] = sparkSession.sparkContext.parallelize(
    List(
      NJProducerRecord(1, StreamOne("a", 0)),
      NJProducerRecord(2, StreamOne("b", 1)),
      NJProducerRecord(3, StreamOne("c", 2)),
      NJProducerRecord(4, StreamOne("d", 3)),
      NJProducerRecord(5, StreamOne("e", 4))
    ))

  val t2Data: RDD[NJProducerRecord[Int, TableTwo]] = sparkSession.sparkContext.parallelize(
    List(
      NJProducerRecord(1, TableTwo("x", 0)),
      NJProducerRecord(2, TableTwo("y", 1)),
      NJProducerRecord(3, TableTwo("z", 2))
    ))

}

class KafkaStreamingTest extends AnyFunSuite {
  import KafkaStreamingCases._

  val stm: KafkaTopic[IO, Int, StreamOne]    = ctx.topic[Int, StreamOne]("stream-one")
  val tab: KafkaTopic[IO, Int, TableTwo]     = ctx.topic[Int, TableTwo]("table-two")
  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream-target")

  implicit val oneValue: Serde[StreamOne]    = stm.codec.valSerde
  implicit val twoValue: Serde[TableTwo]     = tab.codec.valSerde
  implicit val tgtValue: Serde[StreamTarget] = tgt.codec.valSerde

  test("stream-table join") {
    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- stm.kafkaStream.kstream
      b <- tab.kafkaStream.ktable
    } yield a.join(b)((v1, v2) => StreamTarget(v1.name, v2.name, v1.size, v2.color)).to(tgt)

    val prepare: IO[Unit] = for {
      _ <- stm.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- stm.admin.newTopic(1, 1).attempt
      _ <- tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- tab.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- tab.sparKafka.prRdd(t2Data).upload.compile.drain
    } yield ()

    val runStream = for {
      _ <- ctx.runStreams(top)
      _ <- stm.sparKafka.prRdd(s1Data).batch(1).interval(100).upload
    } yield ()

    val rst = tgt.sparKafka.fromKafka.flatMap(_.count.map(c => assert(c == 3, "target error"))) >>
      tab.sparKafka.fromKafka.flatMap(_.count.map(c => assert(c == 3, "table error")))

    (prepare >> runStream.interruptAfter(3.seconds).compile.drain >> rst).unsafeRunSync()
  }
  ignore("streaming") {}
}
