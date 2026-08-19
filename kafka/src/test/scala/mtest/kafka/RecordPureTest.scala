package mtest.kafka

import cats.Bitraverse
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.record.*
import com.github.chenharryhua.nanjin.kafka.serdes.BiTransform
import fs2.kafka.{Header, ProducerRecord}
import io.circe.syntax.given
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.into
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.TimestampType as JavaTimestampType
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class RecordPureTest extends AnyFunSuite {

  // ================================
  // NJProducerRecord
  // ================================

  private val basePR: NJProducerRecord[String, String] = NJProducerRecord(
    topic = "test-topic",
    partition = Some(2),
    offset = Some(100L),
    timestamp = Some(1000L),
    headers = List(NJHeader("h1", List(1, 2, 3))),
    key = Some("key1"),
    value = Some("42")
  )

  test("1.NJProducerRecord builder methods") {
    val tn = TopicName("new-topic")
    assert(basePR.withTopicName(tn).topic === "new-topic")
    assert(basePR.withPartition(5).partition === Some(5))
    assert(basePR.withTimestamp(2000L).timestamp === Some(2000L))
    assert(basePR.withKey("k2").key === Some("k2"))
    assert(basePR.withValue("99").value === Some("99"))
    assert(basePR.withHeaders(Nil).headers === Nil)
  }

  test("2.NJProducerRecord no* methods") {
    assert(basePR.noPartition.partition === None)
    assert(basePR.noTimestamp.timestamp === None)
    assert(basePR.noHeaders.headers === Nil)
    assert(basePR.noKey.key === None)
    assert(basePR.noValue.value === None)
    val noMeta = basePR.noMeta
    assert(noMeta.partition === None)
    assert(noMeta.timestamp === None)
    assert(noMeta.headers === Nil)
  }

  test("3.NJProducerRecord JSON round-trip") {
    val json = basePR.asJson
    val decoded = json.as[NJProducerRecord[String, String]]
    assert(decoded.isRight)
    assert(decoded.toOption.get === basePR)
  }

  test("4.NJProducerRecord Eq instance") {
    assert(basePR === basePR)
    assert(!(basePR === basePR.withKey("other")))
  }

  test("5.NJProducerRecord Bitraverse bimap") {
    val mapped = Bitraverse[NJProducerRecord].bimap(basePR)(_.toUpperCase, _.toUpperCase)
    assert(mapped.key === Some("KEY1"))
    assert(mapped.value === Some("42"))
  }

  test("6.NJProducerRecord to/from JavaProducerRecord round-trip") {
    val pr: NJProducerRecord[String, String] = basePR
    val javaPR: JavaProducerRecord[String, String] = pr.toJavaProducerRecord
    assert(javaPR.topic() === "test-topic")
    assert(javaPR.partition() === 2)
    assert(javaPR.timestamp() === 1000L)
    assert(javaPR.key() === "key1")
    assert(javaPR.value() === "42")

    val backToNJ = NJProducerRecord(javaPR)
    assert(backToNJ.topic === pr.topic)
    assert(backToNJ.key === pr.key)
    assert(backToNJ.value === pr.value)
    assert(backToNJ.partition === pr.partition)
    assert(backToNJ.timestamp === pr.timestamp)
  }

  test("7.NJProducerRecord to/from fs2 ProducerRecord round-trip") {
    val pr: NJProducerRecord[String, String] = basePR
    val fs2PR: ProducerRecord[String, String] = pr.toProducerRecord
    assert(fs2PR.topic === "test-topic")
    assert(fs2PR.key === "key1")
    assert(fs2PR.value === "42")
    assert(fs2PR.partition === Some(2))
    assert(fs2PR.timestamp === Some(1000L))

    val backToNJ = NJProducerRecord(fs2PR)
    assert(backToNJ.topic === pr.topic)
    assert(backToNJ.key === pr.key)
    assert(backToNJ.value === pr.value)
    assert(backToNJ.partition === pr.partition)
    assert(backToNJ.timestamp === pr.timestamp)
  }

  test("8.NJProducerRecord from TopicName factory") {
    val pr = NJProducerRecord(TopicName("t"), "k", "v")
    assert(pr.topic === "t")
    assert(pr.key === Some("k"))
    assert(pr.value === Some("v"))
    assert(pr.partition === None)
    assert(pr.timestamp === None)
    assert(pr.headers === Nil)
  }

  // ================================
  // NJConsumerRecord
  // ================================

  private val baseCR: NJConsumerRecord[String, String] = NJConsumerRecord(
    topic = "test-topic",
    partition = 1,
    offset = 50L,
    timestamp = 2000L,
    timestampType = 0, // CREATE_TIME
    headers = List(NJHeader("hk", List(10, 20))),
    leaderEpoch = Some(5),
    serializedKeySize = 4,
    serializedValueSize = 5,
    key = Some("key1"),
    value = Some("val1")
  )

  test("9.NJConsumerRecord JSON round-trip") {
    val json = baseCR.asJson
    val decoded = json.as[NJConsumerRecord[String, String]]
    assert(decoded.isRight)
    assert(decoded.toOption.get === baseCR)
  }

  test("10.NJConsumerRecord flatten methods") {
    val nested: NJConsumerRecord[Option[String], Option[Int]] = NJConsumerRecord(
      topic = "t",
      partition = 0,
      offset = 0,
      timestamp = 0,
      timestampType = 0,
      headers = Nil,
      leaderEpoch = None,
      serializedKeySize = -1,
      serializedValueSize = -1,
      key = Some(Some("k")),
      value = Some(Some(1))
    )
    val flattened = nested.flatten
    assert(flattened.key === Some("k"))
    assert(flattened.value === Some(1))

    val noneNested: NJConsumerRecord[Option[String], Option[Int]] =
      nested.copy(key = Some(None), value = Some(None))
    val flatNone = noneNested.flatten
    assert(flatNone.key === None)
    assert(flatNone.value === None)
  }

  test("11.NJConsumerRecord flattenKey and flattenValue") {
    val nested: NJConsumerRecord[Option[String], String] = NJConsumerRecord(
      topic = "t",
      partition = 0,
      offset = 0,
      timestamp = 0,
      timestampType = 0,
      headers = Nil,
      leaderEpoch = None,
      serializedKeySize = -1,
      serializedValueSize = -1,
      key = Some(Some("k")),
      value = Some("v")
    )
    assert(nested.flattenKey.key === Some("k"))

    val nestedV: NJConsumerRecord[String, Option[Int]] = NJConsumerRecord(
      topic = "t",
      partition = 0,
      offset = 0,
      timestamp = 0,
      timestampType = 0,
      headers = Nil,
      leaderEpoch = None,
      serializedKeySize = -1,
      serializedValueSize = -1,
      key = Some("k"),
      value = Some(Some(42))
    )
    assert(nestedV.flattenValue.value === Some(42))
  }

  test("12.NJConsumerRecord toNJProducerRecord") {
    val pr = baseCR.toNJProducerRecord
    assert(pr.topic === baseCR.topic)
    assert(pr.partition === Some(baseCR.partition))
    assert(pr.offset === Some(baseCR.offset))
    assert(pr.timestamp === Some(baseCR.timestamp))
    assert(pr.key === baseCR.key)
    assert(pr.value === baseCR.value)
    assert(pr.headers === baseCR.headers)
  }

  test("13.NJConsumerRecord zoned") {
    val zoneId = ZoneId.of("UTC")
    val zoned = baseCR.zoned(zoneId)
    assert(zoned.topic === baseCR.topic)
    assert(zoned.key === baseCR.key)
    assert(zoned.value === baseCR.value)
    assert(zoned.timestamp.toInstant.toEpochMilli === baseCR.timestamp)
  }

  test("14.NJConsumerRecord to/from JavaConsumerRecord round-trip") {
    val javaCR = baseCR.toJavaConsumerRecord
    assert(javaCR.topic() === "test-topic")
    assert(javaCR.partition() === 1)
    assert(javaCR.offset() === 50L)
    assert(javaCR.key() === "key1")
    assert(javaCR.value() === "val1")
    assert(javaCR.timestampType() === JavaTimestampType.CREATE_TIME)

    val backToNJ = NJConsumerRecord(javaCR)
    assert(backToNJ === baseCR)
  }

  test("15.NJConsumerRecord to/from fs2 ConsumerRecord round-trip") {
    val fs2CR = baseCR.toConsumerRecord
    assert(fs2CR.topic === "test-topic")
    assert(fs2CR.partition === 1)
    assert(fs2CR.offset === 50L)
    assert(fs2CR.key === "key1")
    assert(fs2CR.value === "val1")

    val backToNJ = NJConsumerRecord(fs2CR)
    assert(backToNJ.topic === baseCR.topic)
    assert(backToNJ.key === baseCR.key)
    assert(backToNJ.value === baseCR.value)
    assert(backToNJ.partition === baseCR.partition)
    assert(backToNJ.offset === baseCR.offset)
  }

  test("16.NJConsumerRecord timestampType LOG_APPEND_TIME round-trip") {
    val cr = baseCR.copy(timestampType = 1) // LOG_APPEND_TIME
    val javaCR = cr.toJavaConsumerRecord
    assert(javaCR.timestampType() === JavaTimestampType.LOG_APPEND_TIME)
    val back = NJConsumerRecord(javaCR)
    assert(back.timestampType === 1)
  }

  test("17.NJConsumerRecord timestampType NO_TIMESTAMP_TYPE round-trip") {
    val cr = baseCR.copy(timestampType = -1) // unknown/no type
    val javaCR = cr.toJavaConsumerRecord
    assert(javaCR.timestampType() === JavaTimestampType.NO_TIMESTAMP_TYPE)
    val back = NJConsumerRecord(javaCR)
    assert(back.timestampType === JavaTimestampType.NO_TIMESTAMP_TYPE.id)
  }

  test("18.NJConsumerRecord Bitraverse bimap") {
    val mapped = Bitraverse[NJConsumerRecord].bimap(baseCR)(_.length, _.toUpperCase)
    assert(mapped.key === Some(4))
    assert(mapped.value === Some("VAL1"))
  }

  // ================================
  // BiTransform primitives
  // ================================

  test("19.BiTransform Long option") {
    val bi = summon[BiTransform[java.lang.Long, Option[Long]]]
    assert(bi.to(java.lang.Long.valueOf(99L)) === Some(99L))
    assert(bi.from(Some(99L)) === java.lang.Long.valueOf(99L))
    assert(bi.from(None) == null)
  }

  test("20.BiTransform Float option") {
    val bi = summon[BiTransform[java.lang.Float, Option[Float]]]
    assert(bi.to(java.lang.Float.valueOf(1.5f)) === Some(1.5f))
    assert(bi.from(Some(1.5f)) === java.lang.Float.valueOf(1.5f))
    assert(bi.from(None) == null)
  }

  test("21.BiTransform Short option") {
    val bi = summon[BiTransform[java.lang.Short, Option[Short]]]
    assert(bi.to(java.lang.Short.valueOf(7.toShort)) === Some(7.toShort))
    assert(bi.from(Some(7.toShort)) === java.lang.Short.valueOf(7.toShort))
    assert(bi.from(None) == null)
  }

  test("22.BiTransform Double option") {
    val bi = summon[BiTransform[java.lang.Double, Option[Double]]]
    assert(bi.to(java.lang.Double.valueOf(3.14)) === Some(3.14))
    assert(bi.from(Some(3.14)) === java.lang.Double.valueOf(3.14))
    assert(bi.from(None) == null)
  }

  test("23.BiTransform Boolean option") {
    val bi = summon[BiTransform[java.lang.Boolean, Option[Boolean]]]
    assert(bi.to(java.lang.Boolean.TRUE) === Some(true))
    assert(bi.from(Some(false)) === java.lang.Boolean.FALSE)
    assert(bi.from(None) == null)
  }

  test("24.BiTransform Json round-trip") {
    case class Foo(x: Int, y: String)
    given JsonEncoder[Foo] = io.circe.generic.semiauto.deriveEncoder[Foo]
    given JsonDecoder[Foo] = io.circe.generic.semiauto.deriveDecoder[Foo]

    val bi = summon[BiTransform[Json, Foo]]
    val foo = Foo(1, "hello")
    val json = bi.from(foo)
    assert(json.hcursor.get[Int]("x").toOption.contains(1))
    assert(bi.to(json) === foo)
  }

  test("25.BiTransform Option lifting") {
    val lifted = summon[BiTransform[Option[java.lang.Integer], Option[Option[Int]]]]
    assert(lifted.to(Some(Integer.valueOf(5))) === Some(Some(5)))
    assert(lifted.to(None) === None)
    assert(lifted.from(Some(Some(5))) === Some(Integer.valueOf(5)))
    assert(lifted.from(None) === None)
  }

  // ================================
  // RecordTransform (Producer records)
  // ================================

  test("26.JavaProducerRecord to/from fs2 ProducerRecord round-trip") {
    import com.github.chenharryhua.nanjin.kafka.record.given
    val headers = new org.apache.kafka.common.header.internals.RecordHeaders(
      Array[org.apache.kafka.common.header.Header](new RecordHeader("hk", Array[Byte](1, 2))))
    val javaPR = new JavaProducerRecord[String, String]("topic1", 3, 5000L, "key", "value", headers)

    val transformer1 = summon[Transformer[JavaProducerRecord[String, String], ProducerRecord[String, String]]]
    val fs2PR: ProducerRecord[String, String] = transformer1.transform(javaPR)
    assert(fs2PR.topic === "topic1")
    assert(fs2PR.key === "key")
    assert(fs2PR.value === "value")
    assert(fs2PR.partition === Some(3))
    assert(fs2PR.timestamp === Some(5000L))

    val transformer2 = summon[Transformer[ProducerRecord[String, String], JavaProducerRecord[String, String]]]
    val backToJava: JavaProducerRecord[String, String] = transformer2.transform(fs2PR)
    assert(backToJava.topic() === "topic1")
    assert(backToJava.key() === "key")
    assert(backToJava.value() === "value")
    assert(backToJava.partition() === 3)
    assert(backToJava.timestamp() === 5000L)
  }

  test("27.JavaProducerRecord with null partition/timestamp converts correctly") {
    import com.github.chenharryhua.nanjin.kafka.record.given
    val javaPR = new JavaProducerRecord[String, String]("topic2", "key2", "value2")

    val transformer = summon[Transformer[JavaProducerRecord[String, String], ProducerRecord[String, String]]]
    val fs2PR: ProducerRecord[String, String] = transformer.transform(javaPR)
    assert(fs2PR.partition === None)
    assert(fs2PR.timestamp === None)
  }

  // ================================
  // NJHeader round-trip
  // ================================

  test("28.NJHeader to/from fs2 Header round-trip") {
    val njh = NJHeader("myKey", List(10, 20, 30))
    val header: Header = njh.into[Header].transform
    assert(header.key === "myKey")

    val back: NJHeader = header.into[NJHeader].transform
    assert(back.key === njh.key)
    assert(back.value === njh.value)
  }
}
