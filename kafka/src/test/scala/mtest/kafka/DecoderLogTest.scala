package mtest.kafka

import cats.instances.all._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopicDescription, TopicDef}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite

class DecoderLogTest extends AnyFunSuite {
  val data: Array[Byte]    = Array[Byte](1, 2, 3, 4)
  val badData: Array[Byte] = Array[Byte](1, 2, 3, 4, 5)

  val desc: KafkaTopicDescription[Int, Int] = TopicDef[Int, Int]("topic").in(ctx).description

  test("should decode good data") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, data, data)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.isEmpty)
    assert(rst._2.key === Some(16909060))
    assert(rst._2.value === Some(16909060))
  }

  test("should ignore null key") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, null, data)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.isEmpty)
    assert(rst._2.key === None)
    assert(rst._2.value === Some(16909060))
  }

  test("should report null value") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, data, null)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.size === 1)
    assert(rst._2.key === Some(16909060))
    assert(rst._2.value === None)
  }

  test("should report bad key") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, badData, data)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.size === 1)
    assert(rst._2.key === None)
    assert(rst._2.value === Some(16909060))
  }

  test("should report bad value") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, data, badData)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.size === 1)
    assert(rst._2.key === Some(16909060))
    assert(rst._2.value === None)
  }

  test("should report bad key and bad value") {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0, badData, badData)

    val rst = desc.decoder(cr).logRecord.run
    assert(rst._1.size === 2)
    assert(rst._2.key === None)
    assert(rst._2.value === None)
  }
}
