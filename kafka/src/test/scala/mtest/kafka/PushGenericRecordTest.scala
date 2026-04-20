package mtest.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.record.NJProducerRecord
import com.github.chenharryhua.nanjin.kafka.schema.KafkaAvroSchema
import com.sksamuel.avro4s.SchemaFor
import org.scalatest.funsuite.AnyFunSuite

class PushGenericRecordTest extends AnyFunSuite {
  private val topicName: TopicName = TopicName("push.generic.record.test")
  test("schema") {
    val nj = NJProducerRecord[Foo, Int](topicName, Foo(1, "a"), 1)

    val delete = ctx.schemaRegistry.delete(topicName) >>
      ctx.admin(topicName).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence)

    val push = ctx.produceGenericRecord(
      topicName,
      key = Some(SchemaFor[Foo].schema),
      value = Some(SchemaFor[Int].schema)
    ).produceOne(nj.toGenericRecord)

    val schema =
      delete >> push >>
        ctx.schemaRegistry.fetchOptionalAvroSchema(topicName).debug()

    val res = schema.unsafeRunSync()
    assert(res.key.get == KafkaAvroSchema[Foo].schema)
    assert(res.value.isEmpty)
  }
}
