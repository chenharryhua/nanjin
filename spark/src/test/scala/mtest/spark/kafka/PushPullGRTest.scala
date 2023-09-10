package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.GRCodec
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Record
import eu.timepit.refined.auto.*
import fs2.Stream
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

  val baseData: Stream[IO, Record] = Stream
    .range(0, 10)
    .map { a =>
      val record = new GenericData.Record(base)
      record.put("a", a)
      baseTopic.producerFormat.toRecord(a, record)
    }
    .covary[IO]

  val evolveTopic: TopicDef[Int, GenericRecord] =
    TopicDef[Int, GenericRecord](topicName, GRCodec(evolve))

  val evolveData: Stream[IO, Record] = Stream
    .range(10, 20)
    .map { a =>
      val record = new GenericData.Record(evolve)
      record.put("a", a)
      record.put("b", "b")
      evolveTopic.producerFormat.toRecord(a, record)
    }
    .covary[IO]

  test("push - pull - base") {
    val sink = ctx.sink(topicName).build
    val run =
      ctx.schemaRegistry.register(baseTopic) >> (baseData ++ evolveData).chunks.through(sink).compile.drain >>
        ctx.schemaRegistry.fetchAvroSchema(topicName).map(_.value).flatMap(IO.println)

    run.unsafeRunSync()
    sparKafka.dump(topicName, root / "base", NJDateTimeRange(sydneyTime)).unsafeRunSync()

  }

  test("push - pull - evolve") {
    val sink = ctx.sink(topicName).build

    val run =
      ctx.schemaRegistry.register(evolveTopic) >>
        (baseData ++ evolveData).chunks.through(sink).compile.drain >>
        ctx.schemaRegistry.fetchAvroSchema(topicName).map(_.value).flatMap(IO.println)

    run.unsafeRunSync()
    sparKafka.dump(topicName, root / "evolve", NJDateTimeRange(sydneyTime)).unsafeRunSync()
  }

}
