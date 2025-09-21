package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.{immigrate, AvroFor}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder, Record, RecordFormat}
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

  val baseTopic: AvroTopic[Int, version1.Tiger] =
    AvroTopic[Int, version1.Tiger](topicName)(AvroFor[Int], AvroFor[version1.Tiger])
  val evolveTopic: AvroTopic[Int, version2.Tiger] =
    AvroTopic[Int, version2.Tiger](topicName)(AvroFor[Int], AvroFor[version2.Tiger])

  val rfb = RecordFormat[NJProducerRecord[Int, version1.Tiger]]
  val baseData: Stream[IO, Record] =
    Stream
      .range(0, 10)
      .map(a => rfb.to(NJProducerRecord(baseTopic.producerRecord(a, version1.Tiger(a)))))
      .covary[IO]

  val rfe = RecordFormat[NJProducerRecord[Int, version2.Tiger]]
  val evolveData: Stream[IO, Record] =
    Stream
      .range(10, 20)
      .map(a => rfe.to(NJProducerRecord(evolveTopic.producerRecord(a, version2.Tiger(a, Some("b"))))))
      .covary[IO]

  test("push - pull - base") {
    val sink = ctx.produceGenericRecord(baseTopic).sink
    val path = root / "base"
    (ctx.schemaRegistry.register(baseTopic) >> ctx.schemaRegistry.register(evolveTopic)).unsafeRunSync()
    (baseData ++ evolveData).through(sink).compile.drain.unsafeRunSync()
    sparKafka
      .topic(baseTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version1.Tiger]]).jackson(path).run[IO])
      .unsafeRunSync()

    sparKafka.topic(baseTopic).load.jackson(path).count[IO]("c").unsafeRunSync()
    // sparKafka.topic(evolveTopic).load.jackson(path).count.unsafeRunSync()
    Stream // immigration
      .eval(hadoop.filesIn(path))
      .flatMap(
        _.map(hadoop.source(_).jackson(100, baseTopic.pair.optionalAvroSchemaPair.toPair.consumerSchema))
          .reduce(_ ++ _)
          .evalTap(r =>
            IO(assert(r.getSchema == baseTopic.pair.optionalAvroSchemaPair.toPair.consumerSchema)))
          .map(r => immigrate(evolveTopic.pair.optionalAvroSchemaPair.toPair.consumerSchema, r))
          .evalTap(r =>
            IO(assert(r.get.getSchema == evolveTopic.pair.optionalAvroSchemaPair.toPair.consumerSchema))))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("push - pull - evolve") {
    val sink = ctx.produceGenericRecord(evolveTopic).sink
    val path = root / "evolve"

    (baseData ++ evolveData).through(sink).compile.drain.unsafeRunSync()
    sparKafka
      .topic(evolveTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version2.Tiger]]).jackson(path).run[IO])
      .unsafeRunSync()

    //  sparKafka.topic(baseTopic).load.jackson(path).count.unsafeRunSync()
    sparKafka.topic(evolveTopic).load.jackson(path).count[IO]("c").unsafeRunSync()

  }

  test("avro") {
    val pb = root / "avro" / "base"
    val pe = root / "avro" / "evolve"

    sparKafka
      .topic(baseTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version1.Tiger]]).avro(pb).run[IO])
      .unsafeRunSync()
    sparKafka
      .topic(evolveTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version2.Tiger]]).avro(pe).run[IO])
      .unsafeRunSync()

    sparKafka.topic(evolveTopic).load.avro(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(evolveTopic).load.avro(pe).count[IO]("c").unsafeRunSync()

    sparKafka.topic(baseTopic).load.avro(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(baseTopic).load.avro(pe).count[IO]("c").unsafeRunSync()
  }

  test("parquet") {
    val pb = root / "parquet" / "base"
    val pe = root / "parquet" / "evolve"

    sparKafka
      .topic(baseTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version1.Tiger]]).parquet(pb).run[IO])
      .unsafeRunSync()
    sparKafka
      .topic(evolveTopic)
      .fromKafka
      .flatMap(_.rdd.output(Encoder[NJConsumerRecord[Int, version2.Tiger]]).parquet(pe).run[IO])
      .unsafeRunSync()

    sparKafka.topic(evolveTopic).load.parquet(pb).count[IO]("c").unsafeRunSync()
    sparKafka.topic(evolveTopic).load.parquet(pe).count[IO]("c").unsafeRunSync()

    sparKafka.topic(baseTopic).load.parquet(pb).count[IO]("c").unsafeRunSync()
    // sparKafka.topic(baseTopic).load.parquet(pe).count.unsafeRunSync()
  }
}
