package mtest
import mtest.pb.test.Lion

import scala.util.Random

object TestData {
  case class Tigger(id: Int, zooName: String)

  val tiggers: List[Tigger] =
    (1 to 10).map(x => Tigger(Random.nextInt(), "ChengDu Zoo")).toList

  val lions: List[Lion] =
    (1 to 10).map(x => Lion("Melbourne Zoo", Random.nextInt())).toList
}
