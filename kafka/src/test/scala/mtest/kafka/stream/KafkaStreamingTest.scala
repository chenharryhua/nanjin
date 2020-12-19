package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import fs2.Stream
import fs2.kafka.ProducerRecord
import mtest.kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)

  case class TableTwo(name: String, color: Int)

  case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

  val s1Topic: KafkaTopic[IO, Int, StreamOne] = ctx.topic[Int, StreamOne]("stream-one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]  = ctx.topic[Int, TableTwo]("table-two")
  val tgt: KafkaTopic[IO, Int, StreamTarget]  = ctx.topic[Int, StreamTarget]("stream-target")

  val s1Data: List[ProducerRecord[Int, StreamOne]] =
    List(
      ProducerRecord(s1Topic.topicName.value, 1, StreamOne("a", 0)),
      ProducerRecord(s1Topic.topicName.value, 2, StreamOne("b", 1)),
      ProducerRecord(s1Topic.topicName.value, 3, StreamOne("c", 2)),
      ProducerRecord(s1Topic.topicName.value, 4, StreamOne("d", 3)),
      ProducerRecord(s1Topic.topicName.value, 5, StreamOne("e", 4))
    )

  val t2Data: List[ProducerRecord[Int, TableTwo]] =
    List(
      ProducerRecord(t2Topic.topicName.value, 1, TableTwo("x", 0)),
      ProducerRecord(t2Topic.topicName.value, 2, TableTwo("y", 1)),
      ProducerRecord(t2Topic.topicName.value, 3, TableTwo("z", 2))
    )

  val expected: Set[StreamTarget] = Set(
    StreamTarget("a", "x", 0, 0),
    StreamTarget("b", "y", 1, 1),
    StreamTarget("c", "z", 2, 2)
  )
}

class KafkaStreamingTest extends AnyFunSuite {
  import KafkaStreamingData._

  implicit val oneValue: Serde[StreamOne]    = s1Topic.codec.valSerde
  implicit val twoValue: Serde[TableTwo]     = t2Topic.codec.valSerde
  implicit val tgtValue: Serde[StreamTarget] = tgt.codec.valSerde

  test("stream-table join") {
    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.kafkaStream.kstream
      b <- t2Topic.kafkaStream.ktable
    } yield a.join(b)((v1, v2) => StreamTarget(v1.name, v2.name, v1.size, v2.color)).to(tgt)

    val prepare: IO[Unit] = for {
      a <- s1Topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      b <- s1Topic.admin.newTopic(1, 1).attempt
      c <- tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      d <- t2Topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      e <- t2Topic.send(t2Data) // populate table
    } yield {
      println(s"delete s1 topic:   $a")
      println(s"create s1 topic:   $b")
      println(s"delete tgt topic:  $c")
      println(s"delete t2 topic:   $d")
      println(s"populate t2 topic: $e")
    }

    val runStream = for {
      _ <- Stream.eval(prepare)
      _ <- ctx.runStreams(top).delayBy(2.seconds) // start kafka stream
      _ <- s1Data.traverse(d =>
        Stream.every[IO](1.second).evalMap(_ => s1Topic.send(d))) // send msg every second
      d <- tgt.fs2Channel.stream.map(x => tgt.decoder(x).decode.record.value) // harvest the result
    } yield d

    val rst = runStream.interruptAfter(8.seconds).compile.toList.unsafeRunSync().toSet
    assert(rst == expected)
  }
}
