package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.{immigrate, AvroCodec}
import com.sksamuel.avro4s.Record
import eu.timepit.refined.auto.*
import fs2.Stream
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

object version1 {
  final case class Tiger(a: Int)
}
object version2 {
  final case class Tiger(a: Int, b: Option[String] = None)
}

class PushPullGRTest extends AnyFunSuite {

  val topicName: TopicName = TopicName("pull.test")

  val root = "./data/test/spark/kafka/push_pull"

  val baseTopic: TopicDef[Int, version1.Tiger] =
    TopicDef[Int, version1.Tiger](topicName, AvroCodec[version1.Tiger])
  val evolveTopic: TopicDef[Int, version2.Tiger] =
    TopicDef[Int, version2.Tiger](topicName, AvroCodec[version2.Tiger])

  val baseData: Stream[IO, Record] =
    Stream.range(0, 10).map(a => baseTopic.producerFormat.toRecord(a, version1.Tiger(a))).covary[IO]

  val evolveData: Stream[IO, Record] =
    Stream
      .range(10, 20)
      .map(a => evolveTopic.producerFormat.toRecord(a, version2.Tiger(a, Some("b"))))
      .covary[IO]

  test("push - pull - base") {
    val sink = ctx.sink("pull.test", identity)
    val path = root / "base"
    (ctx.schemaRegistry.register(baseTopic) >> ctx.schemaRegistry.register(evolveTopic)).unsafeRunSync()
    (baseData ++ evolveData).chunks.through(sink).compile.drain.unsafeRunSync()
    sparKafka.topic(baseTopic).fromKafka.flatMap(_.output.jackson(path).run[IO]).unsafeRunSync()

    sparKafka.topic(baseTopic).load.jackson(path).count[IO]("c").unsafeRunSync()
    // sparKafka.topic(evolveTopic).load.jackson(path).count.unsafeRunSync()
    Stream // immigration
      .eval(hadoop.filesIn(path))
      .flatMap(
        _.map(hadoop.source(_).jackson(100, baseTopic.schemaPair.consumerSchema))
          .reduce(_ ++ _)
          .evalTap(r => IO(assert(r.getSchema == baseTopic.schemaPair.consumerSchema)))
          .map(r => immigrate(evolveTopic.schemaPair.consumerSchema, r))
          .evalTap(r => IO(assert(r.get.getSchema == evolveTopic.schemaPair.consumerSchema))))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("push - pull - evolve") {
    val sink = ctx.sink(evolveTopic.topicName, identity)
    val path = root / "evolve"

    (baseData ++ evolveData).chunks.through(sink).compile.drain.unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.flatMap(_.output.jackson(path).run[IO]).unsafeRunSync()

    //  sparKafka.topic(baseTopic).load.jackson(path).count.unsafeRunSync()
    sparKafka.topic(evolveTopic).load.jackson(path).count[IO]("c").unsafeRunSync()

  }

  test("avro") {
    val pb = root / "avro" / "base"
    val pe = root / "avro" / "evolve"

    sparKafka.topic(baseTopic).fromKafka.flatMap(_.output.avro(pb).run[IO]).unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.flatMap(_.output.avro(pe).run[IO]).unsafeRunSync()

    sparKafka.topic(evolveTopic).load.avro(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(evolveTopic).load.avro(pe).count[IO]("c").unsafeRunSync()

    sparKafka.topic(baseTopic).load.avro(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(baseTopic).load.avro(pe).count[IO]("c").unsafeRunSync()
  }

  test("parquet") {
    val pb = root / "parquet" / "base"
    val pe = root / "parquet" / "evolve"

    sparKafka.topic(baseTopic).fromKafka.flatMap(_.output.parquet(pb).run[IO]).unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.flatMap(_.output.parquet(pe).run[IO]).unsafeRunSync()

    sparKafka.topic(evolveTopic).load.parquet(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(evolveTopic).load.parquet(pe).count[IO]("c").unsafeRunSync()

    sparKafka.topic(baseTopic).load.parquet(pb).count[IO]("c").unsafeRunSync()
    // sparKafka.topic(baseTopic).load.parquet(pe).count.unsafeRunSync()
  }
}
