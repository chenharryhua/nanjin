package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonSchemaTopic}
import com.github.chenharryhua.nanjin.spark.RddExt
import eu.timepit.refined.auto.*
import fs2.Stream
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

class KJsonTest extends AnyFunSuite {
  val avro = AvroTopic[Int, Simple]("spark-avro-simple")
  val json = JsonSchemaTopic[Int, Simple]("spark-json-simple")

  val data: List[(Int, Simple)] = List.range(0, 10).map(a => a -> Simple("simple", a))

  val root = "./data/test/spark/kafka/kjson"

  test("load - unload") {
    Stream
      .emits(data)
      .chunks
      .broadcastThrough(ctx.produce(avro).sink, ctx.produce(json).sink)
      .compile
      .drain
      .unsafeRunSync()

    val res = sparKafka.topic(avro).fromKafka.flatMap(_.rdd.output.circe(root / "circe").run[IO]) >>
      sparKafka.topic(json).fromKafka.flatMap(_.rdd.output.circe(root / "circe").run[IO])

    res.unsafeRunSync()
  }
}
