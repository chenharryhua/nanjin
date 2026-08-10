package mtest.kafka
import cats.implicits.toBifunctorOps
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.record.*
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

  test("1.toJsonNode: basic fields") {
    val node: JsonNode =
      objectMapper.valueToTree[JsonNode](
        base.bimap(
          k => objectMapper.valueToTree(k),
          v => objectMapper.valueToTree(v)
        ))

    assert(node.get("topic").asText() == "test-topic")
    assert(node.get("partition").asInt() == 1)
    assert(node.get("offset").asLong() == 100L)
    assert(node.get("timestamp").asLong() == 123456789L)
  }

  test("2.toJsonNode: key/value mapping") {
    val node: JsonNode =
      objectMapper.valueToTree(
        base.bimap(
          k => objectMapper.valueToTree(k.toUpperCase),
          v => objectMapper.valueToTree(v.reverse)
        ))

    assert(node.get("key").asText() == "K")
    assert(node.get("value").asText() == "v".reverse)
  }

  test("3.toJsonNode: None key/value becomes null") {
    val record = base.copy(key = None, value = None)

    val node: JsonNode =
      objectMapper.valueToTree(record.bimap(identity, identity))

    assert(node.get("key").isNull)
    assert(node.get("value").isNull)
  }

  test("4.toJsonNode: partial mapping") {
    val node: JsonNode =
      objectMapper.valueToTree(
        base.bimap(
          k => objectMapper.valueToTree(k),
          v => objectMapper.valueToTree(v)
        ))

    assert(node.get("key").asText() == "k")
    assert(node.get("value").asText() == "v")
  }

  test("5.toJsonNode: headers serialized") {
    val node: JsonNode =
      objectMapper.valueToTree(
        base.bimap(
          k => objectMapper.valueToTree(k),
          v => objectMapper.valueToTree(v)
        ))

    val headers = node.get("headers")
    assert(headers.size() == 1)

    val h = headers.get(0)
    assert(h.get("key").asText() == "h1")
  }

  test("6.toJsonNode: leaderEpoch optional") {
    val node: JsonNode =
      objectMapper.valueToTree(
        base.bimap(
          k => objectMapper.valueToTree(k),
          v => objectMapper.valueToTree(v)
        ))

    assert(node.get("leaderEpoch").asInt() == 3)

    val noEpoch: JsonNode =
      objectMapper.valueToTree(
        base.copy(leaderEpoch = None)
          .bimap(
            k => objectMapper.valueToTree(k),
            v => objectMapper.valueToTree(v)
          ))

    assert(noEpoch.get("leaderEpoch").isNull)
  }

  test("7.codec derives") {
    summon[Encoder[NJConsumerRecord[Json, Json]]]
    summon[Decoder[NJConsumerRecord[Json, Json]]]
    summon[Decoder[ZonedConsumerRecord[Json, Int]]]
    summon[Encoder[ZonedConsumerRecord[Int, Int]]]
  }
}
