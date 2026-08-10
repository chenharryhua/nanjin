package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaProtobufSchema, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef, TopicName}
import com.google.protobuf.DynamicMessage
import org.scalatest.funsuite.AnyFunSuite

class UpAndDownProtoTest extends AnyFunSuite {
  private val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(_.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(_.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
    )

  private val topic = TopicName("up.and.down.proto")
  private val proto: TopicDef[Integer, ProtoConsumerRecord] =
    TopicDef(topic, Primitive[Integer], Structured[DynamicMessage].become[ProtoConsumerRecord])

  test("1.proto - schema register") {
    val schema = summon[KafkaProtobufSchema[ProtoConsumerRecord]].schema
    println(schema)
    ctx.schemaRegistry
      .register(topic, value = Some(schema))
      .debug()
      .unsafeRunSync()
  }

  test("2.proto - produce") {
    ctx.produce(proto).produceOne(1, ProtoConsumerRecord("abc")).debug().unsafeRunSync()
  }

  test("3.proto - consume") {
    ctx.consume(proto).subscribe.take(1).debug().compile.drain.unsafeRunSync()
  }

  test("4.get schema") {
    ctx.schemaRegistry.fetchOptionalJsonSchema(proto.topicName).debug().unsafeRunSync()
    // ctx.schemaRegistry.delete(json.topicName).unsafeRunSync()
    // ctx.admin(json.topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence).unsafeRunSync()
  }
}
