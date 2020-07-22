package mtest.spark

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}

object CoproductTestData {

  final case class Apple(size: Int, locale: String)
  final case class WaterMelon(size: Int, weight: Float)
  type Fruit = Apple :+: WaterMelon :+: CNil
  final case class Food(fruit: Fruit, num: Int)

  val food = List(
    Food(Coproduct[Fruit](Apple(1, "beijing")), 1),
    Food(Coproduct[Fruit](WaterMelon(1, 1.1f)), 2),
    Food(Coproduct[Fruit](Apple(2, "shanghai")), 3))
}

class CoproductTest extends AnyFunSuite {
  import CoproductTestData._
  test("coproduct avro") {
    val path = "./data/test/spark/coproduct/food.avro"
    val prepare =
      Stream.emits(food).covary[IO].through(fileSink[IO](blocker).avro(path)).compile.drain
    (fileSink[IO](blocker).delete(path) >> prepare).unsafeRunSync()
    val rst = sparkSession.avro[Food](path).collect().toSet
    assert(rst == food.toSet)
  }
}
