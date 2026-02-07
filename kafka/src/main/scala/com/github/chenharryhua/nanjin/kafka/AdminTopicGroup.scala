package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import fs2.kafka.{AutoOffsetReset, KafkaAdminClient}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait AdminTopicGroup[F[_]] {

  /** Compute lag behind the latest offsets for a consumer group.
    *
    * @return
    *   a map of topic partitions to optional lag behind values
    */
  def lagBehind: F[TopicPartitionMap[Option[LagBehind]]]

  /** Delete all offsets of a consumer group for this topic.
    *
    * @return
    *   an effect that removes consumer group offsets
    */
  def deleteConsumerGroupOffsets: F[Unit]

  /** Commit offsets synchronously for a consumer group.
    *
    * @param offsets
    *   a map of topic partitions to `OffsetAndMetadata` to commit
    * @return
    *   an effect that commits the offsets
    */
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /** Commit a single offset synchronously for a specific partition.
    *
    * @param partition
    *   the partition index
    * @param offset
    *   the offset to commit
    * @return
    *   an effect that commits the offset
    */
  def commitSync(partition: Int, offset: Long): F[Unit]

  /** Reset offsets for a consumer group to the beginning of the topic.
    *
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsToBegin: F[Unit]

  /** Reset offsets for a consumer group to the end of the topic.
    *
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsToEnd: F[Unit]

  /** Reset offsets for a consumer group to the specified timestamp.
    *
    * @param ts
    *   the target `NJTimestamp`
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsForTimes(ts: NJTimestamp): F[Unit]

}

object AdminTopicGroup {

  def apply[F[_]: Sync](
    adminResource: Resource[F, KafkaAdminClient[F]],
    topicName: TopicName,
    groupId: GroupId,
    consumerSettings: KafkaConsumerSettings): Resource[F, AdminTopicGroup[F]] =
    for {
      admin <- adminResource
      consumer <- SnapshotConsumer(
        topicName,
        PureConsumerSettings
          .withProperties(consumerSettings.properties)
          .withAutoOffsetReset(AutoOffsetReset.None)
          .withEnableAutoCommit(false)
          .withGroupId(groupId.value)
      )
    } yield new AdminTopicGroupImpl(admin, consumer, topicName, groupId)

  final private class AdminTopicGroupImpl[F[_]: Sync](
    adminClient: KafkaAdminClient[F],
    consumerClient: SnapshotConsumer[F],
    topicName: TopicName,
    groupId: GroupId
  ) extends AdminTopicGroup[F] {

    override def lagBehind: F[TopicPartitionMap[Option[LagBehind]]] =
      for {
        ends <- consumerClient.endOffsets
        curr <- adminClient
          .listConsumerGroupOffsets(groupId.value)
          .partitionsToOffsetAndMetadata
          .map(_.filter(_._1.topic() === topicName.name.value).map { case (k, v) => k -> Offset(v) })
          .map(TopicPartitionMap(_))
      } yield calculate.admin_lagBehind(ends, curr)

    override def deleteConsumerGroupOffsets: F[Unit] =
      for {
        tps <- consumerClient.partitionsFor
        _ <- adminClient.deleteConsumerGroupOffsets(groupId.value, tps.value.toSet)
      } yield ()

    override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      consumerClient.commitSync(offsets)

    override def commitSync(partition: Int, offset: Long): F[Unit] = {
      val tp = new TopicPartition(topicName.name.value, partition)
      val oam = new OffsetAndMetadata(offset)
      commitSync(Map(tp -> oam))
    }

    override def resetOffsetsToBegin: F[Unit] =
      consumerClient.resetOffsetsToBegin

    override def resetOffsetsToEnd: F[Unit] =
      consumerClient.resetOffsetsToEnd

    override def resetOffsetsForTimes(ts: NJTimestamp): F[Unit] =
      consumerClient.resetOffsetsForTimes(ts)

  }
}
