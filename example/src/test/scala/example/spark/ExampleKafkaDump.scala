package example.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic, ProtoTopic}
import eu.timepit.refined.auto.*
import example.kafka.JsonLion
import fs2.Stream
import io.scalaland.chimney.dsl.TransformerOps
import mtest.pb.test.Lion
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ExampleKafkaDump extends AnyFunSuite {
  val avro = AvroTopic[Long, JsonLion]("spark-avro")
  val sjson = JsonTopic[Long, JsonLion]("spark-json-schema")
  val proto = ProtoTopic[Long, Lion]("spark-protobuf")

  val lions = Stream.emits(List.fill(10)(Lion("lion", 0))).covary[IO]
  val jlions = lions.map(_.transformInto[JsonLion]).zipWithIndex.map(_.swap)

  test("upload") {
    jlions.chunks
      .through(example.ctx.produce(avro).sink)
      .concurrently(jlions.chunks.through(example.ctx.produce(sjson).sink))
      .concurrently(lions.zipWithIndex.map(_.swap).chunks.through(example.ctx.produce(proto).sink))
      .compile
      .drain
      .unsafeRunSync()
  }
}
