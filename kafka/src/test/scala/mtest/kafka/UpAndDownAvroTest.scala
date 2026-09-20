package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaCodec, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import org.apache.avro.generic.GenericRecord
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

final case class UpAndDown(a: Int, b: String)

class UpAndDownAvroTest extends CatsEffectSuite {
  private val topic = TopicName("up.and.down.avro")
  private val avro: TopicDef[Integer, UpAndDown] =
    TopicDef(topic.value, Primitive[Integer], Structured[GenericRecord].become[UpAndDown])

  test("1.avro - schema register") {
    val schema = KafkaCodec.avro[UpAndDown].schema
    ctx.schemaRegistry(topic.value)
      .register(value = Some(schema))
  }

  test("2.avro - produce") {
    ctx.produce(avro).produceOne(1, UpAndDown(1, "a")).void
  }

  test("3.avro - consume") {
    ctx.consume(avro).subscribe.take(1).timeout(3.seconds).compile.drain
  }

}
