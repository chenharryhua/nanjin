package mtest.kafka

import cats.derived.auto.show._
import cats.implicits._
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

sealed trait Color2
final case class Red(str: String, i: Int) extends Color2
final case class Green(str: String) extends Color2
final case class Blue(str: String) extends Color2
final case class Cloth(color: Color2, name: String, size: Int)

class KAvroTest extends AnyFunSuite {

  val topic = ctx.topic[Int, Cloth]("cloth")
  test("should support coproduct") {
    val b = Cloth(Blue("b"), "blue-cloth", 1)
    val r = Cloth(Red("r", 1), "red-cloth", 2)
    val g = Cloth(Green("g"), "green-cloth", 3)
    val run =
      topic.admin.idefinitelyWantToDeleteTheTopic >> topic.send(List(1 -> r, 2 -> g, 3 -> b)) *>
        topic.consumerResource.use(_.retrieveLastRecords.map(m => topic.decoder(m.head).decode))

    assert(run.unsafeRunSync().value() === b)
  }
}
