package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaCodec, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

final case class UpAndDown(a: Int, b: String)

class UpAndDownAvroTest extends AnyFunSuite {
  private val topic = TopicName("up.and.down.avro")
  private val avro: TopicDef[Integer, UpAndDown] =
    TopicDef(topic.value, Primitive[Integer], Structured[GenericRecord].become[UpAndDown])

  test("1.avro - schema register") {
    val schema = KafkaCodec.avro[UpAndDown].schema
    ctx.schemaRegistry
      .register(topic, value = Some(schema))
      .unsafeRunSync()
  }

  test("2.avro - produce") {
    ctx.produce(avro).produceOne(1, UpAndDown(1, "a")).void.unsafeRunSync()
  }

  test("3.avro - consume") {
    ctx.consume(avro).subscribe.take(1).timeout(3.seconds).compile.drain.unsafeRunSync()
  }

}
