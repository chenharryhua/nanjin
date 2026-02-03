package mtest.terminals

import cats.effect.IO
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, ToRecord}
import fs2.Stream
import kantan.csv.generic.*
import kantan.csv.{RowDecoder, RowEncoder}

import scala.util.Random

object TestData {
  case class Tiger(id: Int, zooName: Option[String])

  object Tiger {
    implicit val re: RowEncoder[Tiger] = shapeless.cachedImplicit
    implicit val rd: RowDecoder[Tiger] = shapeless.cachedImplicit
    val avroEncoder: Encoder[Tiger] = Encoder[Tiger]
    val avroDecoder: Decoder[Tiger] = Decoder[Tiger]
    val from: FromRecord[Tiger] = FromRecord[Tiger]
    val to: ToRecord[Tiger] = ToRecord[Tiger]
  }

  val tigers: List[Tiger] =
    (1 to 10).map(i => Tiger(i, if (Random.nextBoolean()) Some("ChengDu Zoo") else None)).toList

  val tigerSet: Set[Tiger] = tigers.toSet

  val herd_number: Long = 10000L
  val herd: Stream[IO, Tiger] =
    Stream.range(0, herd_number).map(idx => TestData.Tiger(idx.toInt, Some("zoo"))).covary[IO]

}
