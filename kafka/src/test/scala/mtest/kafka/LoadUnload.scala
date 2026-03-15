package mtest.kafka

import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import com.github.chenharryhua.nanjin.kafka.TopicName

final case class Simple(name: String, count: Int)

class LoadUnload extends AnyFunSuite {

  val avro: AvroTopic[Integer, Simple] = AvroTopic[Integer, Simple](TopicName("spark-avro-simple"))
  val json: JsonTopic[Integer, Simple] = JsonTopic[Integer, Simple](TopicName("spark-json-simple"))

  val data: List[(Integer, Simple)] =
    List.range(1, 10).map(a => Integer.valueOf(a) -> Simple("simple", Random.nextInt(99)))

  test("load - unload") {
    val init = ctx.schemaRegistry.register(avro) >>
      ctx.schemaRegistry.register(json) >>
      Stream.emits(data).broadcastThrough(ctx.produce(avro).sink, ctx.produce(json).sink).compile.drain

    init.unsafeRunSync()

    val a = ctx
      .consume(avro)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .toList
    val b = ctx
      .consume(json)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .toList

    val res = (a, b).mapN(_ == _).unsafeRunSync()
    assert(res)
  }
}
