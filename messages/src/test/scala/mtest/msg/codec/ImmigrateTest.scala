package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.immigrate
import com.sksamuel.avro4s.{Record, RecordFormat, SchemaFor}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

object version1 {
  final case class Tiger(a: Int)
}
object version2 {
  final case class Cat(a: Int)
}
object version3 {
  final case class Tiger(a: Int, b: Option[String])
}
object version4 {
  final case class Tiger(a: Int, b: Option[String] = None)
}
object version5 {
  final case class Lion(a: String)
}

object version6 {
  final case class Key(a: Int)
  final case class KeyBoard(key: Key)
}
object version7 {
  final case class Key2(a: Int, b: Option[String] = None)
  final case class KeyBoard2(key: Key2, b: Option[String] = None)
}

class ImmigrateTest extends AnyFunSuite {
  val v1s: Schema = SchemaFor[version1.Tiger].schema
  val v2s: Schema = SchemaFor[version2.Cat].schema
  val v3s: Schema = SchemaFor[version3.Tiger].schema
  val v4s: Schema = SchemaFor[version4.Tiger].schema
  val v5s: Schema = SchemaFor[version5.Lion].schema
  val v6s: Schema = SchemaFor[version6.KeyBoard].schema
  val v7s: Schema = SchemaFor[version7.KeyBoard2].schema

  val v1d: Record = RecordFormat[version1.Tiger].to(version1.Tiger(1))
  val v2d: Record = RecordFormat[version2.Cat].to(version2.Cat(1))
  val v3d: Record = RecordFormat[version3.Tiger].to(version3.Tiger(1, Some("b")))
  val v4d: Record = RecordFormat[version4.Tiger].to(version4.Tiger(1, Some("c")))
  val v5d: Record = RecordFormat[version5.Lion].to(version5.Lion("lion"))
  val v6d: Record = RecordFormat[version6.KeyBoard].to(version6.KeyBoard(version6.Key(0)))
  val v7d: Record = RecordFormat[version7.KeyBoard2].to(version7.KeyBoard2(version7.Key2(0)))

  test("immigration") {
    assert(immigrate(v1s, v1d).isSuccess)
    assert(immigrate(v1s, v2d).isSuccess)
    assert(immigrate(v1s, v3d).isSuccess)
    assert(immigrate(v1s, v4d).isSuccess)
    assert(immigrate(v1s, v5d).isFailure)

    assert(immigrate(v2s, v1d).isSuccess)
    assert(immigrate(v2s, v2d).isSuccess)
    assert(immigrate(v2s, v3d).isSuccess)
    assert(immigrate(v2s, v4d).isSuccess)
    assert(immigrate(v2s, v5d).isFailure)

    assert(immigrate(v3s, v1d).isFailure)
    assert(immigrate(v3s, v2d).isFailure)
    assert(immigrate(v3s, v3d).isSuccess)
    assert(immigrate(v3s, v4d).isSuccess)
    assert(immigrate(v3s, v5d).isFailure)

    assert(immigrate(v4s, v1d).isSuccess)
    assert(immigrate(v4s, v2d).isSuccess)
    assert(immigrate(v4s, v3d).isSuccess)
    assert(immigrate(v4s, v4d).isSuccess)
    assert(immigrate(v4s, v5d).isFailure)

    assert(immigrate(v6s, v7d).isSuccess)
    assert(immigrate(v7s, v6d).isSuccess)

  }

  test("immigrate null") {
    assert(immigrate(v2s, null).get == null)
  }
}
