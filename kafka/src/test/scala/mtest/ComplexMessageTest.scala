package mtest

import java.time.{Instant, LocalDateTime}

import cats.Show
import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.datetime._
import io.circe.generic.JsonCodec
import io.circe.generic.auto._
import io.circe.shapes._
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil}
import io.circe.Encoder
import io.circe.Decoder
import shapeless.Coproduct

final case class Employee2(name: String, age: Int, department: String)

@JsonCodec sealed trait SealedTrait

object SealedTrait {

  implicit val showColorish: Show[SealedTrait] = new Show[SealedTrait] {
    override def show(t: SealedTrait): String = t.toString
  }
  case object Red extends SealedTrait
  case object Green extends SealedTrait
  case object Blue extends SealedTrait
  case class AbiColor(r: Int, g: Int, b: Int) extends SealedTrait
}

@JsonCodec case class Annotated(a: Int, b: Int)

object EnumTest extends Enumeration {
  type Materials = Value
  val Wood, Steel, Stone                      = Value
  implicit val show: Show[Value]              = _.toString
  implicit val matEncoder: Encoder[Materials] = io.circe.Encoder.encodeEnumeration(EnumTest)
  implicit val matDecoder: Decoder[Materials] = io.circe.Decoder.decodeEnumeration(EnumTest)
}

@JsonCodec final case class ComplexMessage(
  a: Int        = 0,
  b: String     = "a",
  c: Float      = 1.0f,
  d: BigDecimal = 2.0,
  e: Double     = 3.0d,
  f: LocalDateTime,
  g: Instant,
  h: Employee2,
  m: Annotated,
  i: EnumTest.Materials,
  j: Int :+: String :+: Float :+: CNil,
  k: SealedTrait,
  l: Annotated :+: Employee2 :+: Int :+: CNil
)

class ComplexMessageTest extends AnyFunSuite {

  val topic = TopicDef[Int, ComplexMessage]("complex-msg-test")

  val m = ComplexMessage(
    1,
    "b",
    1.0f,
    2.0,
    3.0d,
    LocalDateTime.now,
    Instant.now(),
    Employee2("e", 10, "tb"),
    Annotated(1, 2),
    EnumTest.Steel,
    Coproduct[Int :+: String :+: Float :+: CNil]("aaaaa"),
    SealedTrait.Blue,
    Coproduct[Annotated :+: Employee2 :+: Int :+: CNil](Annotated(-1, -1))
  )
  test("identical") {
    println(m.asJson.toString)
    assert(m.asJson.as[ComplexMessage].toOption.get == m)
  }
}
