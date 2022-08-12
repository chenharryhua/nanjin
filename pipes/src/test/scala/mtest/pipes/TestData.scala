package mtest.pipes

import com.sksamuel.avro4s.{Decoder, Encoder}
import kantan.csv.generic.*
import kantan.csv.{RowDecoder, RowEncoder}

import scala.util.Random

object TestData {
  case class Tiger(id: Int, zooName: Option[String])

  object Tiger {
    implicit val re: RowEncoder[Tiger] = shapeless.cachedImplicit
    implicit val rd: RowDecoder[Tiger] = shapeless.cachedImplicit
    val avroEncoder: Encoder[Tiger]    = Encoder[Tiger]
    val avroDecoder: Decoder[Tiger]    = Decoder[Tiger]
  }

  val tigers: List[Tiger] =
    (1 to 10)
      .map(_ => Tiger(Random.nextInt(), if (Random.nextBoolean()) Some("ChengDu Zoo") else None))
      .toList

  val tigerSet: Set[Tiger] = tigers.toSet
}
