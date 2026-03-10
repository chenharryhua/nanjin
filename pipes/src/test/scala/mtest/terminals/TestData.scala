package mtest.terminals

import cats.effect.IO
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import fs2.Stream

import scala.util.Random

object TestData {
  case class Tiger(id: Int, zooName: Option[String])

  object Tiger {
    val avroEncoder: Encoder[Tiger] = Encoder[Tiger]
    val avroDecoder: Decoder[Tiger] = Decoder[Tiger]
    val from: FromRecord[Tiger] = FromRecord[Tiger](SchemaFor[Tiger].schema)
    val to: ToRecord[Tiger] = ToRecord[Tiger](SchemaFor[Tiger].schema)
  }

  val tigers: List[Tiger] =
    1.to(10).toList.map(i => Tiger(i, if (Random.nextBoolean()) Some("ChengDu Zoo") else None)).toList

  val tigerSet: Set[Tiger] = tigers.toSet

  val herd_number: Long = 10000L
  val herd: Stream[IO, Tiger] =
    Stream.range(0L, herd_number).map(idx => TestData.Tiger(idx.toInt, Some("zoo"))).covary[IO]

}
