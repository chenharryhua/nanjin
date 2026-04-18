package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.schema.KafkaJsonSchema
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings, TopicDef, TopicName}
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class UpAndDownJsonTest extends AnyFunSuite {
  private val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
        .withSerdeProperty(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, classOf[JsonNode].getName)
    )

  private val topic = TopicName("up.and.down.json2")
  private val json: TopicDef[Integer, UpAndDown] =
    TopicDef(topic, Primitive[Integer], Structured[JsonNode].become[UpAndDown])

  test("json - schema register") {
    val schema = summon[KafkaJsonSchema[UpAndDown]].schema
    println(schema)
    ctx.schemaRegistry
      .register(topic, value = Some(schema))
      .debug()
      .unsafeRunSync()
  }

  test("json - produce") {
    ctx.produce(json).produceOne(1, UpAndDown(3, "abc")).debug().unsafeRunSync()
  }

  test("json - consume") {
    ctx.consume(json).subscribe.take(1).debug().timeout(3.seconds).compile.drain.unsafeRunSync()
  }

  test("get schema") {
    ctx.schemaRegistry.fetchOptionalJsonSchema(json.topicName).debug().unsafeRunSync()
    // ctx.schemaRegistry.delete(json.topicName).unsafeRunSync()
    // ctx.admin(json.topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence).unsafeRunSync()
  }
}
