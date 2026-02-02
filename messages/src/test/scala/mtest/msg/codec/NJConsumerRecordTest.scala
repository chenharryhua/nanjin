package mtest.msg.codec

import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroFor
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJHeader}
import mtest.msg.codec.ManualAvroSchemaTestData.UnderTest
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordTest extends AnyFunSuite {
  val cr: NJConsumerRecord[Int, UnderTest] = NJConsumerRecord[Int, UnderTest](
    topic = "test",
    partition = 12,
    offset = 34,
    timestamp = 100,
    timestampType = 0,
    headers = List(NJHeader("key", "value".getBytes.toList)),
    leaderEpoch = None,
    serializedKeySize = 10,
    serializedValueSize = 10,
    key = Some(1),
    value = Some(UnderTest(1, "b"))
  )

  test("to zoned json") {
    println(cr.toZonedJson(sydneyTime).noSpaces)
  }

  test("schema") {
    println(AvroFor[UnderTest].schema)
  }

}
