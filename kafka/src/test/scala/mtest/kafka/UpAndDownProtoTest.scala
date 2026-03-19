package mtest.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.github.chenharryhua.nanjin.kafka.schema.KafkaProtobufSchema
import com.github.chenharryhua.nanjin.kafka.serdes.{isoDynamicMessage, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.google.protobuf.DynamicMessage
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class UpAndDownProtoTest extends AnyFunSuite {
  private val topic = TopicName("up.and.down.proto")
  private val proto: TopicDef[Integer, ProtoConsumerRecord] =
    TopicDef(
      topic,
      Primitive[Integer],
      Structured[DynamicMessage].iso(isoDynamicMessage[ProtoConsumerRecord]))

  test("proto - schema register") {
    val schema = summon[KafkaProtobufSchema[ProtoConsumerRecord]].schema
    println(schema)
    ctx.schemaRegistry
      .register(topic, value = Some(schema))
      .debug()
      .unsafeRunSync()
  }

  test("proto - produce") {
    ctx.produce(proto).produceOne(1, ProtoConsumerRecord("abc")).debug().unsafeRunSync()
  }

  test("proto - consume") {
    ctx.consume(proto).subscribe.take(1).debug().timeout(3.seconds).compile.drain.unsafeRunSync()
  }
}
