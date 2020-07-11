package mtest.avro

import com.github.chenharryhua.nanjin.messages.avro.{KJson, SerdeOf}
import org.scalatest.funsuite.AnyFunSuite
import shapeless.test.illTyped

object KJsonCompilationTestData {
  final case class Base(a: Int, b: String)
  final case class CompositionType(a: Int, b: String, c: Double, base: Base)
}

class KJsonCompilationTest extends AnyFunSuite {
  import KJsonCompilationTestData._

  test("KJson should be well-typed if circe is imported") {
    import io.circe.generic.auto._
    val goodJson = SerdeOf[KJson[CompositionType]]
  }
}
