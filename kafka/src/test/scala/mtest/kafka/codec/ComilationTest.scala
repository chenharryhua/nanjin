package mtest.kafka.codec

import com.github.chenharryhua.nanjin.kafka.codec.SerdeOf
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, SerdeOf}
import org.scalatest.funsuite.AnyFunSuite
import shapeless.test.illTyped

class ComilationTest extends AnyFunSuite {
  test("KJson should be ill-typed if circe is not imported") {
    illTyped("SerdeOf[KJson[PrimitiveTypeCombined]]")
  }
  test("KJson should be well-typed if circe is imported") {
    import io.circe.generic.auto._
    val goodJson = SerdeOf[KJson[PrimitiveTypeCombined]]
  }
}
