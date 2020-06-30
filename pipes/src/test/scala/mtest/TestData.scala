package mtest

import cats.implicits._

import scala.util.Random

object TestData {
  case class Tigger(id: Int, zooName: String)

  val tiggers: List[Tigger] =
    (1 to 10).map(x => Tigger(Random.nextInt(), "ChengDu zoo")).toList
}
