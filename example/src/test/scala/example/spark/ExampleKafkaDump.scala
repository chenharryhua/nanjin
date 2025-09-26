package example.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonLightTopic, JsonSchemaTopic, ProtobufTopic}
import com.github.chenharryhua.nanjin.spark.RddExt
import eu.timepit.refined.auto.*
import example.kafka.JsonLion
import example.sparKafka
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import io.scalaland.chimney.dsl.TransformerOps
import mtest.pb.test.Lion
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite


@DoNotDiscover
class ExampleKafkaDump extends AnyFunSuite {
  val avro = AvroTopic[Long, JsonLion]("spark-avro")
  val sjson = JsonSchemaTopic[Long, JsonLion]("spark-json-schema")
  val ljson = JsonLightTopic[Long, JsonLion]("spark-json-light")
  val proto = ProtobufTopic[Long, Lion]("spark-protobuf")

  val lions = Stream.emits(List.fill(10)(Lion("lion", 0))).covary[IO]
  val jlions = lions.map(_.transformInto[JsonLion]).zipWithIndex.map(_.swap)

  test("upload") {
    jlions.chunks
      .through(example.ctx.produce(avro).sink)
      .concurrently(jlions.chunks.through(example.ctx.produce(sjson).sink))
      .concurrently(jlions.chunks.through(example.ctx.produce(ljson).sink))
      .concurrently(lions.zipWithIndex.map(_.swap).chunks.through(example.ctx.produce(proto).sink))
      .compile
      .drain
      .unsafeRunSync()
  }
  test("spark dump") {
    val path = Url.parse("./data/example/spark/dump")
    val d1 =
      sparKafka.topic(avro).fromKafka.flatMap(_.rdd.out.avro(path / "1").withCompression(_.Snappy).run[IO])

    val d2 =
      sparKafka.topic(sjson).fromKafka.flatMap(_.rdd.out.avro(path / "2").withCompression(_.Snappy).run[IO])

    val d3 =
      sparKafka.topic(ljson).fromKafka.flatMap(_.rdd.out.avro(path / "3").withCompression(_.Snappy).run[IO])

    val d4 =
      sparKafka
        .topic(proto)
        .fromKafka
        .flatMap(
          _.rdd
            .map(_.bimap(identity, _.transformInto[JsonLion]))
            .out
            .avro(path / "4")
            .run[IO])

    (d1 >> d2 >> d3 >> d4).unsafeRunSync()
  }
}
