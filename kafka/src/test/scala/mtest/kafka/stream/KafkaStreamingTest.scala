package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerResult}
import mtest.kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)

  case class TableTwo(name: String, color: Int)
  case class GlobalThree(name: String, weight: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Topic: KafkaTopic[IO, Int, StreamOne]   = ctx.topic[Int, StreamOne]("stream-one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]    = ctx.topic[Int, TableTwo]("table-two")
  val g3Topic: KafkaTopic[IO, Int, GlobalThree] = ctx.topic[Int, GlobalThree]("global-three")

  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream-join-target")

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

  val g3Data: List[ProducerRecord[Int, GlobalThree]] =
    List(
      ProducerRecord(g3Topic.topicName.value, 4, GlobalThree("gx", 1000)),
      ProducerRecord(g3Topic.topicName.value, 5, GlobalThree("gy", 2000)),
      ProducerRecord(g3Topic.topicName.value, 6, GlobalThree("gz", 3000))
    )

  val expected: Set[StreamTarget] = Set(
    StreamTarget("a", 0, 0),
    StreamTarget("b", 0, 1),
    StreamTarget("c", 0, 2),
    StreamTarget("d", 1000, 0),
    StreamTarget("e", 2000, 0)
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
      c <- g3Topic.kafkaStream.gktable
    } yield {
      a.join(c)((i, s1) => i, (s1, g3) => StreamTarget(s1.name, g3.weight, 0)).to(tgt) 
      a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt)
    }

    val prepare: IO[Unit] = for {
      a <- s1Topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      b <- s1Topic.admin.newTopic(1, 1).attempt
      c <- tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      d <- t2Topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      f <- t2Topic.send(t2Data) // populate table
      g <- g3Topic.send(g3Data)
    } yield {
      println(s"delete s1 topic:   $a")
      println(s"create s1 topic:   $b")
      println(s"delete tgt topic:  $c")
      println(s"delete t2 topic:   $d")
      println(s"populate t2 topic: $f")
      println(s"populate g3 topic: $g")
    }

    val populateS1Topic: Stream[IO, ProducerResult[Int, StreamOne, Unit]] = Stream
      .every[IO](1.seconds)
      .zipRight(Stream.emits(s1Data))
      .evalMap(s1Topic.send)
      .delayBy(1.seconds)

    val streamingService: Stream[IO, KafkaStreams] =
      ctx.runStreams(top).handleErrorWith(_ => Stream.sleep_(2.seconds) ++ ctx.runStreams(top))

    val harvest: Stream[IO, StreamTarget] =
      tgt.fs2Channel.stream.map(x => tgt.decoder(x).decode.record.value).interruptAfter(2.seconds)

    val runStream = for {
      _ <- Stream.eval(prepare)
      _ <- streamingService.concurrently(populateS1Topic)
      d <- harvest
    } yield d

    val res = runStream.compile.toList.unsafeRunSync().toSet

    assert(res == expected)
  }
}
