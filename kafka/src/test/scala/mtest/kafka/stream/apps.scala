package mtest.kafka.stream

import cats.implicits.showInterpolator
import mtest.kafka.stream.KafkaStreamingData.{StreamOne, StreamTarget, TableTwo}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.serialization.Serdes.intSerde
import cats.derived.auto.show.*
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaContext

object apps {
  def app1(ctx: KafkaContext[IO])(sb: StreamsBuilder): Unit = {
    implicit val ev1: Serde[StreamTarget] = ctx.asValue[StreamTarget]
    implicit val ev2: Serde[StreamOne] = ctx.asValue[StreamOne]
    implicit val ev3: Serde[TableTwo] = ctx.asValue[TableTwo]
    val a = sb.stream[Int, StreamOne]("stream.test.join.stream.one")
    val b = sb.table[Int, TableTwo]("stream.test.join.table.two")
    a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to("stream.test.join.target")
  }
}
