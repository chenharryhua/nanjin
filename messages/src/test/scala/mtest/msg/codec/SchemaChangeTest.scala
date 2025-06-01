package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{AvroDoc, AvroNamespace}
import eu.timepit.refined.auto.*
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}

object SchemaChangeTestData {
  @AvroNamespace("schema.test.nest")
  final case class Nest(a: Int)
  @AvroNamespace("schema.test.nest2")
  @AvroDoc("nest-2")
  final case class Nest2(b: String)
  @AvroNamespace("schema.test.top")
  @AvroDoc("top level case class")
  final case class UnderTest(a: Int, b: Nest :+: Nest2 :+: CNil, c: Option[Int] = None)

  val schema =
    """
{"type":"record","name":"UnderTest","namespace":"schema.test.top","doc":"top level case class","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","namespace":"schema.test.nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","namespace":"schema.test.nest2","doc":"nest-2","fields":[{"name":"b","type":"string"}]}]},{"name":"c","type":["null","int"],"default":null}]}    """

  val oldSchema: Schema           = (new Schema.Parser).parse(schema)
  val codec: AvroCodec[UnderTest] = AvroCodec[UnderTest](schema)

}

class SchemaChangeTest extends AnyFunSuite {
  import SchemaChangeTestData.*

  test("remove default field") {
    val newCodec: AvroCodec[UnderTest] = codec.withoutDefaultField

    val s =
      """
{"type":"record","name":"UnderTest","namespace":"schema.test.top","doc":"top level case class","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","namespace":"schema.test.nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","namespace":"schema.test.nest2","doc":"nest-2","fields":[{"name":"b","type":"string"}]}]},{"name":"c","type":["null","int"]}]}        """
    assert(newCodec.schema.toString == s.trim)
    val data = UnderTest(1, Coproduct(Nest(1)))
    val en   = codec.encode(data)
    val res  = newCodec.decode(en)

    assert(res == data)
  }

  test("change namespace") {
    val newCodec: AvroCodec[UnderTest] = codec.withNamespace("new.namespace")
    val s                              =
      """
    {"type":"record","name":"UnderTest","namespace":"new.namespace","doc":"top level case class","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","doc":"nest-2","fields":[{"name":"b","type":"string"}]}]},{"name":"c","type":["null","int"],"default":null}]}"""
    assert(newCodec.schema.toString() == s.trim)
    val data = UnderTest(1, Coproduct(Nest(1)), Some(1))
    val en   = newCodec.encode(data)
    val res  = newCodec.decode(en)
    assert(res == data)
  }

  test("remove namespace") {
    val newCodec: AvroCodec[UnderTest] = codec.withoutNamespace
    val s                              =
      """
{"type":"record","name":"UnderTest","doc":"top level case class","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","doc":"nest-2","fields":[{"name":"b","type":"string"}]}]},{"name":"c","type":["null","int"],"default":null}]}      """
    assert(newCodec.schema.toString() == s.trim)
    val data = UnderTest(1, Coproduct(Nest(1)), Some(1))
    val en   = newCodec.encode(data)
    val res  = newCodec.decode(en)

    assert(res == data)
  }

  test("remove namespace - 1") {
    val newCodec: AvroCodec[UnderTest] = codec.withoutNamespace

    val data = UnderTest(1, Coproduct(Nest(1)), Some(1))
    val en   = newCodec.encode(data)
    assertThrows[Exception](codec.decode(en))
  }
  test("remove namespace - 2") {
    val newCodec: AvroCodec[UnderTest] = codec.withoutNamespace

    val data = UnderTest(1, Coproduct(Nest(1)), Some(1))
    val en   = codec.encode(data)
    assertThrows[Exception](newCodec.decode(en))
  }

  test("remove doc") {
    val newCodec: AvroCodec[UnderTest] = codec.withoutDoc
    val s                              =
      """
{"type":"record","name":"UnderTest","namespace":"schema.test.top","fields":[{"name":"a","type":"int"},{"name":"b","type":[{"type":"record","name":"Nest","namespace":"schema.test.nest","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"Nest2","namespace":"schema.test.nest2","fields":[{"name":"b","type":"string"}]}]},{"name":"c","type":["null","int"],"default":null}]}"""
    assert(newCodec.schema.toString() == s.trim)
    val data = UnderTest(1, Coproduct(Nest(1)), Some(1))
    val en   = newCodec.encode(data)
    val res  = newCodec.decode(en)

    assert(res == data)
  }
}
