package mtest.msg.codec

import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroFor
import com.github.chenharryhua.nanjin.messages.kafka.{globalObjectMapper, NJConsumerRecord, NJHeader}
import mtest.msg.codec.ManualAvroSchemaTestData.UnderTest
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordTest extends AnyFunSuite {
  val cr = NJConsumerRecord[Int, UnderTest](
    topic = "test",
    partition = 12,
    offset = 34,
    timestamp = 100,
    timestampType = 0,
    headers = List(NJHeader("key", "value".getBytes.toList)),
    leaderEpoch = None,
    serializedKeySize = Some(10),
    serializedValueSize = Some(10),
    key = Some(1),
    value = Some(UnderTest(1, "b"))
  )

  test("from/to json node") {
    val res = cr.toJsonNode
    println(res)
    println(cr.toZonedJson(sydneyTime).noSpaces)
    assert(io.circe.jawn.decode[NJConsumerRecord[Int, UnderTest]](res.toPrettyString).toOption.get == cr)
    val tree = globalObjectMapper.readTree(res.toPrettyString)
    val readback = globalObjectMapper.convertValue[NJConsumerRecord[Int, UnderTest]](tree)
    assert(readback === cr)
  }

  test("to zoned json") {
    println(cr.toZonedJson(sydneyTime).noSpaces)
  }

  test("schema") {
    println(AvroFor[UnderTest].schema)
  }

}
