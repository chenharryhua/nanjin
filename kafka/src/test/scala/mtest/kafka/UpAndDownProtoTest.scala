package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaCodec, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef, TopicName}
import com.google.protobuf.DynamicMessage
import munit.CatsEffectSuite

class UpAndDownProtoTest extends CatsEffectSuite {
  private val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(_.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(_.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
    )

  private val topic = TopicName("up.and.down.proto")
  private val proto: TopicDef[Integer, ProtoConsumerRecord] =
    TopicDef(topic.value, Primitive[Integer], Structured[DynamicMessage].become[ProtoConsumerRecord])

  test("1.proto - schema register") {
    val schema = KafkaCodec.protobuf[ProtoConsumerRecord].schema
    ctx.schemaRegistry(topic.value)
      .register(value = Some(schema))
  }

  test("2.proto - produce") {
    ctx.produce(proto).produceOne(1, ProtoConsumerRecord("abc")).void
  }

  test("3.proto - consume") {
    ctx.consume(proto).subscribe.take(1).compile.drain
  }

  test("4.get schema") {
    ctx.schemaRegistry(proto.topicName.value).fetchOptionalJsonSchema.void
    // ctx.schemaRegistry.delete(json.topicName).unsafeRunSync()
    // ctx.admin(json.topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence).unsafeRunSync()
  }
}
