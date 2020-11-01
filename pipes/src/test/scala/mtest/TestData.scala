package mtest

import kantan.csv.{RowDecoder, RowEncoder}
import kantan.csv.generic._
import mtest.pb.test.Lion

import scala.util.Random

object TestData {
  case class Tigger(id: Int, zooName: Option[String])

  object Tigger {
    implicit val re: RowEncoder[Tigger] = shapeless.cachedImplicit
    implicit val rd: RowDecoder[Tigger] = shapeless.cachedImplicit
  }

  val tiggers: List[Tigger] =
    (1 to 10)
      .map(x => Tigger(Random.nextInt(), if (Random.nextBoolean) Some("ChengDu Zoo") else None))
      .toList

  val lions: List[Lion] =
    (1 to 10).map(x => Lion("Melbourne Zoo", Random.nextInt())).toList
}
