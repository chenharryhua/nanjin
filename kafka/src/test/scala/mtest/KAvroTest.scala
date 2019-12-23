package mtest

import org.scalatest.funsuite.AnyFunSuite
import cats.Show
import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.codec.bitraverse._
import com.github.chenharryhua.nanjin.codec.show._
import io.circe.generic.auto._

sealed trait Color
final case class Red(str: String, i: Int) extends Color
final case class Green(str: String) extends Color
final case class Blue(str: String) extends Color
final case class Cloth(color: Color, name: String, size: Int)

class KAvroTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Cloth]("cloth")
  test("should support coproduct") {
    val b = Cloth(Blue("b"), "blue-cloth", 1)
    val r = Cloth(Red("r", 1), "red-cloth", 2)
    val g = Cloth(Green("g"), "green-cloth", 3)
    val run =
      topic.producer.send(1, r) >>
        topic.producer.send(2, g) >>
        topic.producer.send(3, b) >>
        topic.consumer.retrieveLastRecords.map(m => topic.decoder(m.head).decode)
    assert(run.unsafeRunSync().value() === b)
  }
  test("should output json and avro") {
    val run = topic.fs2Channel.consume
      .map(x => topic.decoder(x).avro.show)
      .showLinesStdOut
      .take(1)
      .compile
      .drain >>
      topic.fs2Channel.consume
        .map(x => topic.decoder(x).json.show)
        .showLinesStdOut
        .take(1)
        .compile
        .drain
    run.unsafeRunSync()
  }
}
