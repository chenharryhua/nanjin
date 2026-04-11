package com.github.chenharryhua.nanjin.kafka

import cats.syntax.order.given
import cats.{Order, PartialOrder, Show}
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Codec, Decoder, Encoder}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

opaque type TopicName = String
object TopicName:
  def apply(tn: String): TopicName = tn
  extension (tn: TopicName) inline def value: String = tn

  given Show[TopicName] = OpaqueLift.lift[TopicName, String, Show]
  given Encoder[TopicName] = OpaqueLift.lift[TopicName, String, Encoder]
  given Decoder[TopicName] = OpaqueLift.lift[TopicName, String, Decoder]

  given Conversion[String, TopicName] with
    override def apply(tn: String): TopicName = TopicName(tn)
end TopicName

opaque type GroupId = String
object GroupId:
  def apply(value: String): GroupId = value
  extension (gid: GroupId) inline def value: String = gid

  given Show[GroupId] = OpaqueLift.lift[GroupId, String, Show]
  given Encoder[GroupId] = OpaqueLift.lift[GroupId, String, Encoder]
  given Decoder[GroupId] = OpaqueLift.lift[GroupId, String, Decoder]

  given Conversion[String, GroupId] with
    override def apply(gid: String): GroupId = GroupId(gid)
end GroupId

opaque type Offset = Long
object Offset:
  def apply(value: Long): Offset = value
  def apply(oam: OffsetAndMetadata): Offset = Offset(oam.offset())
  def apply(oat: OffsetAndTimestamp): Offset = Offset(oat.offset())

  extension (offset: Offset)
    inline def value: Long = offset
    def asLast: Offset = Offset(Math.max(0, offset - 1))
    def -(other: Offset): Long = offset - other.value

  given Show[Offset] = OpaqueLift.lift[Offset, String, Show]
  given Encoder[Offset] = OpaqueLift.lift[Offset, Long, Encoder]
  given Decoder[Offset] = OpaqueLift.lift[Offset, Long, Decoder]
  given Ordering[Offset] = OpaqueLift.lift[Offset, Long, Ordering]
  given Order[Offset] = OpaqueLift.lift[Offset, Long, Order]
end Offset

opaque type Partition = Int
object Partition:
  def apply(value: Int): Partition = value
  extension (p: Partition)
    inline def value: Int = p
    def -(other: Partition): Int = p - other.value

  given Show[Partition] = OpaqueLift.lift[Partition, Int, Show]
  given Encoder[Partition] = OpaqueLift.lift[Partition, Int, Encoder]
  given Decoder[Partition] = OpaqueLift.lift[Partition, Int, Decoder]
  given Ordering[Partition] = OpaqueLift.lift[Partition, Int, Ordering]
  given Order[Partition] = OpaqueLift.lift[Partition, Int, Order]
end Partition

final case class OffsetRange private (from: Long, until: Long) {
  val distance: Long = until - from
  val to: Long = until - 1
}

object OffsetRange {
  def apply(from: Offset, until: Offset): Option[OffsetRange] =
    if (from < until)
      Some(OffsetRange(from.value, until.value))
    else
      None

  given PartialOrder[OffsetRange] =
    (x: OffsetRange, y: OffsetRange) =>
      (x, y) match {
        case (OffsetRange(xf, xu), OffsetRange(yf, yu)) if xf >= yf && xu < yu =>
          -1.0
        case (OffsetRange(xf, xu), OffsetRange(yf, yu)) if xf === yf && xu === yu =>
          0.0
        case (OffsetRange(xf, xu), OffsetRange(yf, yu)) if xf <= yf && xu > yu =>
          1.0
        case _ => Double.NaN
      }
}

final case class PartitionRange(topicPartition: TopicPartition, offsetRange: OffsetRange) {
  override def toString: String =
    s"${topicPartition.topic()}-${topicPartition.partition()}-${offsetRange.from}-${offsetRange.to}"
}

object PartitionRange {
  given Show[PartitionRange] = Show.fromToString[PartitionRange]
}

final case class LagBehind private (current: Long, end: Long, lag: Long) derives Codec.AsObject
object LagBehind {
  def apply(current: Offset, end: Offset): LagBehind =
    LagBehind(current.value, end.value, end - current)
}

final case class RegisteredSchemaID(key: Option[Int], value: Option[Int])
