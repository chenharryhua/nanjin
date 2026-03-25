package mtest.kafka
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.record.*
import com.github.chenharryhua.nanjin.kafka.serdes.globalObjectMapper
import io.circe.{Decoder, Encoder, Json}
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordJsonTest extends AnyFunSuite {

  val base = NJConsumerRecord[String, String](
    topic = "test-topic",
    partition = 1,
    offset = 100L,
    timestamp = 123456789L,
    timestampType = 0,
    headers = List(NJHeader("h1", List[Byte](1, 2))),
    leaderEpoch = Some(3),
    serializedKeySize = 10,
    serializedValueSize = 20,
    key = Some("k"),
    value = Some("v")
  )

  test("toJsonNode: basic fields") {
    val node: JsonNode =
      base.toJsonNode(
        k => globalObjectMapper.valueToTree(k),
        v => globalObjectMapper.valueToTree(v)
      )

    assert(node.get("topic").asText() == "test-topic")
    assert(node.get("partition").asInt() == 1)
    assert(node.get("offset").asLong() == 100L)
    assert(node.get("timestamp").asLong() == 123456789L)
  }

  test("1.toJsonNode: key/value mapping") {
    val node =
      base.toJsonNode(
        k => globalObjectMapper.valueToTree(k.toUpperCase),
        v => globalObjectMapper.valueToTree(v.reverse)
      )

    assert(node.get("key").asText() == "K")
    assert(node.get("value").asText() == "v".reverse)
  }

  test("2.toJsonNode: None key/value becomes null") {
    val record = base.copy(key = None, value = None)

    val node =
      record.toJsonNode(identity, identity)

    assert(node.get("key").isNull)
    assert(node.get("value").isNull)
  }

  test("3.toJsonNode: partial mapping") {
    val node =
      base.toJsonNode(
        k => globalObjectMapper.valueToTree(k),
        v => globalObjectMapper.valueToTree(v)
      )

    assert(node.get("key").asText() == "k")
    assert(node.get("value").asText() == "v")
  }

  test("4.toJsonNode: headers serialized") {
    val node =
      base.toJsonNode(
        k => globalObjectMapper.valueToTree(k),
        v => globalObjectMapper.valueToTree(v)
      )

    val headers = node.get("headers")
    assert(headers.size() == 1)

    val h = headers.get(0)
    assert(h.get("key").asText() == "h1")
  }

  test("5.toJsonNode: leaderEpoch optional") {
    val node =
      base.toJsonNode(
        k => globalObjectMapper.valueToTree(k),
        v => globalObjectMapper.valueToTree(v)
      )

    assert(node.get("leaderEpoch").asInt() == 3)

    val noEpoch =
      base.copy(leaderEpoch = None)
        .toJsonNode(
          k => globalObjectMapper.valueToTree(k),
          v => globalObjectMapper.valueToTree(v)
        )

    assert(noEpoch.get("leaderEpoch").isNull)
  }

  test("codec derives") {
    summon[Encoder[NJConsumerRecord[Json, Json]]]
    summon[Decoder[NJConsumerRecord[Json, Json]]]
  }
}
