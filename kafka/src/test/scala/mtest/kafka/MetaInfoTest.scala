package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.record.MetaInfo
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

class MetaInfoTest extends AnyFunSuite {

  // Minimal Avro record carrying the fields MetaInfo reads. timestampType is a nullable int so we can
  // exercise both the present and absent cases. Extra/other fields are irrelevant: MetaInfo.apply only
  // reads these by name.
  private val schema: Schema =
    SchemaBuilder
      .record("MetaInfo")
      .namespace("mtest.kafka")
      .fields()
      .name("topic").`type`().stringType().noDefault()
      .name("partition").`type`().intType().noDefault()
      .name("offset").`type`().longType().noDefault()
      .name("timestamp").`type`().longType().noDefault()
      .name("timestampType").`type`().nullable().intType().noDefault()
      .name("serializedKeySize").`type`().intType().noDefault()
      .name("serializedValueSize").`type`().intType().noDefault()
      .endRecord()

  private def record(
    partition: Any = 1,
    offset: Any = 100L,
    timestamp: Any = 1234567890L,
    timestampType: Any = 0,
    serializedKeySize: Any = 10,
    serializedValueSize: Any = 20): GenericData.Record = {
    val r = new GenericData.Record(schema)
    r.put("topic", "test-topic")
    r.put("partition", partition)
    r.put("offset", offset)
    r.put("timestamp", timestamp)
    r.put("timestampType", timestampType)
    r.put("serializedKeySize", serializedKeySize)
    r.put("serializedValueSize", serializedValueSize)
    r
  }

  test("1.parses a well-formed generic record") {
    MetaInfo(record()) match {
      case Success(mi) =>
        assert(mi.topic == "test-topic")
        assert(mi.partition == 1)
        assert(mi.offset == 100L)
        assert(mi.timestamp == 1234567890L)
        assert(mi.timestampType.contains(0))
        assert(mi.serializedKeySize == 10)
        assert(mi.serializedValueSize == 20)
      case Failure(ex) => fail(s"expected success, got $ex")
    }
  }

  test("2.timestampType is None when the field is null") {
    MetaInfo(record(timestampType = null)) match {
      case Success(mi) => assert(mi.timestampType.isEmpty)
      case Failure(ex) => fail(s"expected success, got $ex")
    }
  }

  test("3.timestampType is Some when the field is present") {
    MetaInfo(record(timestampType = 1)) match {
      case Success(mi) => assert(mi.timestampType.contains(1))
      case Failure(ex) => fail(s"expected success, got $ex")
    }
  }

  test("4.a wrong-typed int field fails with a descriptive message") {
    // partition carries a Long where an int is expected
    MetaInfo(record(partition = 5L)) match {
      case Success(mi) => fail(s"expected failure, got $mi")
      case Failure(ex) =>
        assert(ex.isInstanceOf[IllegalArgumentException])
        assert(ex.getMessage.contains("partition"))
        assert(ex.getMessage.contains("expected int"))
    }
  }

  test("5.a long field tolerates an int value (widening)") {
    MetaInfo(record(offset = 42)) match {
      case Success(mi) => assert(mi.offset == 42L)
      case Failure(ex) => fail(s"expected success, got $ex")
    }
  }

  test("6.a wrong-typed long field fails with a descriptive message") {
    // timestamp carries a String where a long is expected
    MetaInfo(record(timestamp = "nope")) match {
      case Success(mi) => fail(s"expected failure, got $mi")
      case Failure(ex) =>
        assert(ex.getMessage.contains("timestamp"))
        assert(ex.getMessage.contains("expected long"))
    }
  }
}
