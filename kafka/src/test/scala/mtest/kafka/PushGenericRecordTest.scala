package mtest.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.record.NJProducerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.KafkaCodec
import com.sksamuel.avro4s.SchemaFor
import org.scalatest.funsuite.AnyFunSuite

class PushGenericRecordTest extends AnyFunSuite {
  private val topicName: TopicName = TopicName("push.generic.record.test")
  test("1.schema") {
    val nj = NJProducerRecord[Foo, Int](topicName, Foo(1, "a"), 1)

    val push = ctx.produceGenericRecord(
      topicName,
      key = Some(SchemaFor[Foo].schema),
      value = Some(SchemaFor[Int].schema)
    ).produceOne(nj.toGenericRecord)

    val schema = ctx.schemaRegistry.delete(topicName) >>
      push >>
      ctx.schemaRegistry.fetchOptionalAvroSchema(topicName)

    val res = schema.unsafeRunSync()
    assert(res.key.get == KafkaCodec.avro[Foo].schema)
    assert(res.value.isEmpty)
  }
}
