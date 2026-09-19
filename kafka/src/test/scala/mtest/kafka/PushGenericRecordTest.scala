package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.record.NJProducerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.KafkaCodec
import com.sksamuel.avro4s.SchemaFor
import munit.CatsEffectSuite

class PushGenericRecordTest extends CatsEffectSuite {
  private val topicName: TopicName = TopicName("push.generic.record.test")
  test("1.schema") {
    val nj = NJProducerRecord[Foo, Int](topicName.value, Foo(1, "a"), 1)

    val push = ctx.produceGenericRecord(
      topicName.value,
      key = Some(SchemaFor[Foo].schema),
      value = Some(SchemaFor[Int].schema)
    ).produceOne(nj.toGenericRecord)

    val schema = ctx.schemaRegistry(topicName.value).delete >>
      push >>
      ctx.schemaRegistry(topicName.value).fetchOptionalAvroSchema

    schema.map { res =>
      assert(res.key.get == KafkaCodec.avro[Foo].schema)
      assert(res.value.isEmpty)
    }
  }
}
