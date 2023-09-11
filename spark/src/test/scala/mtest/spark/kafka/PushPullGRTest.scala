package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.GRCodec
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.ProducerRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.funsuite.AnyFunSuite

class PushPullGRTest extends AnyFunSuite {
  val base: Schema = (new Schema.Parser).parse("""
        {"type":"record","name":"Tiger","namespace":"pull","fields":[{"name":"a","type":"int"}]}
      """)
  val evolve: Schema = (new Schema.Parser).parse("""
      {"type":"record","name":"Tiger","namespace":"pull","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}
    """)

  val topicName: TopicName = TopicName("pull.test")

  val root: NJPath = NJPath("./data/test/spark/kafka/push_pull")

  val baseTopic: TopicDef[Int, GenericRecord] =
    TopicDef[Int, GenericRecord](topicName, GRCodec(base))

  val baseData: Stream[IO, ProducerRecord[Int, GenericRecord]] = Stream
    .range(0, 10)
    .map { a =>
      val record = new GenericData.Record(base)
      record.put("a", a)
      baseTopic.njProducerRecord(a, record).toProducerRecord
    }
    .covary[IO]

  val evolveTopic: TopicDef[Int, GenericRecord] =
    TopicDef[Int, GenericRecord](topicName, GRCodec(evolve))

  val evolveData: Stream[IO, ProducerRecord[Int, GenericRecord]] = Stream
    .range(10, 20)
    .map { a =>
      val record = new GenericData.Record(evolve)
      record.put("a", a)
      record.put("b", "b")
      evolveTopic.producerRecord(a, record)
    }
    .covary[IO]

  test("push - pull - base") {
    val sink = ctx.topic(baseTopic).produce.pipe
    val path = root / "base"
    (baseData ++ evolveData).chunks.through(sink).compile.drain.unsafeRunSync()
    sparKafka.topic(baseTopic).fromKafka.output.jackson(path).run.unsafeRunSync()

    sparKafka.topic(baseTopic).load.jackson(path).count.unsafeRunSync()
   // sparKafka.topic(evolveTopic).load.jackson(path).count.unsafeRunSync()

  }

  test("push - pull - evolve") {
    val sink = ctx.topic(evolveTopic).produce.pipe
    val path = root / "evolve"

    (baseData ++ evolveData).chunks.through(sink).compile.drain.unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.output.jackson(path).run.unsafeRunSync()

   //  sparKafka.topic(baseTopic).load.jackson(path).count.unsafeRunSync()
     sparKafka.topic(evolveTopic).load.jackson(path).count.unsafeRunSync()

  }

  test("avro") {
    val pb = root / "avro" / "base"
    val pe = root / "avro" / "evolve"

    sparKafka.topic(baseTopic).fromKafka.output.avro(pb).run.unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.output.avro(pe).run.unsafeRunSync()

    sparKafka.topic(evolveTopic).load.avro(pb).count.unsafeRunSync()
    sparKafka.topic(evolveTopic).load.avro(pe).count.unsafeRunSync()

    sparKafka.topic(baseTopic).load.avro(pb).count.unsafeRunSync()
    sparKafka.topic(baseTopic).load.avro(pe).count.unsafeRunSync()
  }

  test("parquet") {
    val pb = root / "parquet" / "base"
    val pe = root / "parquet" / "evolve"

    sparKafka.topic(baseTopic).fromKafka.output.parquet(pb).run.unsafeRunSync()
    sparKafka.topic(evolveTopic).fromKafka.output.parquet(pe).run.unsafeRunSync()

    sparKafka.topic(evolveTopic).load.parquet(pb).count.unsafeRunSync()
    sparKafka.topic(evolveTopic).load.parquet(pe).count.unsafeRunSync()

    sparKafka.topic(baseTopic).load.parquet(pb).count.unsafeRunSync()
   // sparKafka.topic(baseTopic).load.parquet(pe).count.unsafeRunSync()
  }
}
