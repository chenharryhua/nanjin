package example.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic, ProtoTopic}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.scalaland.chimney.dsl.TransformerOps
import mtest.pb.test.Lion
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ExampleKafkaDump extends AnyFunSuite {
  val avro = AvroTopic[Long, Cub]("spark-avro")
  val sjson = JsonTopic[Long, Cub]("spark-json-schema")
  val proto = ProtoTopic[Long, Lion]("spark-protobuf")

  val lions = Stream.emits(List.fill(10)(Lion("lion", 0))).covary[IO]
  val jlions = lions.map(_.transformInto[Cub]).zipWithIndex.map(_.swap)

  test("upload") {
    jlions.chunks
      .through(example.ctx.produce(avro).sink)
      .concurrently(jlions.chunks.through(example.ctx.produce(sjson).sink))
      .concurrently(lions.zipWithIndex.map(_.swap).chunks.through(example.ctx.produce(proto).sink))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("schema register") {
    val a = example.ctx.schemaRegistry.fetchOptionalJsonSchema(sjson.topicName).unsafeRunSync()
    val b = example.ctx.schemaRegistry.fetchOptionalAvroSchema(avro.topicName).unsafeRunSync()
    val c = example.ctx.schemaRegistry.fetchOptionalProtobufSchema(proto.topicName).unsafeRunSync()
    assert(a.key.isEmpty)
    assert(a.value.nonEmpty)
    assert(b.key.isEmpty)
    assert(b.value.nonEmpty)
    assert(c.key.isEmpty)
    assert(c.value.nonEmpty)
  }
}
