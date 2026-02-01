package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{DateTimeRange, NJTimestamp}
import fs2.kafka.{AutoOffsetReset, KafkaAdminClient}
import org.apache.kafka.clients.admin.{NewTopic, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaAdminApi[F[_]] {

  /** Permanently deletes the topic associated with this admin instance.
    *
    * WARNING: This operation is **irreversible**. All data in the topic will be lost, and any consumers or
    * producers interacting with the topic may fail.
    *
    * The method name is intentionally verbose to ensure the caller **fully understands the consequences** of
    * invoking it.
    *
    * @return
    *   an effect that deletes the topic; will fail if the topic does not exist
    */
  def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit]

  /** Describe the topic associated with this admin instance.
    *
    * @return
    *   a map of topic name to [[TopicDescription]]; in practice, this will contain a single entry for the
    *   topic
    */
  def describe: F[Map[String, TopicDescription]]

  /** List all consumer groups consuming this topic.
    *
    * @return
    *   a list of [[GroupId]] for all consumer groups that have offsets in this topic
    */
  def groups: F[List[GroupId]]

  /** Compute lag behind the latest offsets for a consumer group.
    *
    * @param groupId
    *   the consumer group id
    * @return
    *   a map of topic partitions to optional lag behind values
    */
  def lagBehind(groupId: String): F[TopicPartitionMap[Option[LagBehind]]]

  /** Create a new topic with the given number of partitions and replication factor.
    *
    * @param numPartition
    *   number of partitions (must be > 0)
    * @param numReplica
    *   replication factor (must be > 0)
    * @return
    *   an effect that creates the topic
    */
  def newTopic(numPartition: Int, numReplica: Short): F[Unit]

  /** Create a new topic based on the given [[TopicDescription]].
    *
    * Assumes `description.partitions()` is non-empty. Will throw if `partitions()` is empty – this is
    * intentional, as a topic must have at least one partition.
    *
    * @param description
    *   the topic description
    * @return
    *   an effect that creates the topic
    */
  def newTopic(description: TopicDescription): F[Unit]

  /** Mirror this topic to another topic.
    *
    * Assumes the source topic has at least one partition. Will throw if `desc.partitions()` is empty –
    * intentional, mirroring requires at least one partition.
    *
    * @param other
    *   the target topic
    * @return
    *   an effect that creates the mirrored topic
    */
  def mirrorTo(other: TopicName): F[Unit]

  /** Delete all offsets of a consumer group for this topic.
    *
    * @param groupId
    *   the consumer group id
    * @return
    *   an effect that removes consumer group offsets
    */
  def deleteConsumerGroupOffsets(groupId: String): F[Unit]

  /** List all partitions of this topic.
    *
    * @return
    *   a [[ListOfTopicPartitions]] containing partition information
    */
  def partitionsFor: F[ListOfTopicPartitions]

  /** Compute offset ranges for the topic for a given time window.
    *
    * @param dtr
    *   the [[DateTimeRange]] for which to compute offsets
    * @return
    *   a map of topic partitions to optional offset ranges
    */
  def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]]

  /** Commit offsets synchronously for a consumer group.
    *
    * @param groupId
    *   the consumer group id
    * @param offsets
    *   a map of topic partitions to [[OffsetAndMetadata]] to commit
    * @return
    *   an effect that commits the offsets
    */
  def commitSync(groupId: String, offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /** Commit a single offset synchronously for a specific partition.
    *
    * @param groupId
    *   the consumer group id
    * @param partition
    *   the partition index
    * @param offset
    *   the offset to commit
    * @return
    *   an effect that commits the offset
    */
  def commitSync(groupId: String, partition: Int, offset: Long): F[Unit]

  /** Reset offsets for a consumer group to the beginning of the topic.
    *
    * @param groupId
    *   the consumer group id
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsToBegin(groupId: String): F[Unit]

  /** Reset offsets for a consumer group to the end of the topic.
    *
    * @param groupId
    *   the consumer group id
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsToEnd(groupId: String): F[Unit]

  /** Reset offsets for a consumer group to the specified timestamp.
    *
    * @param groupId
    *   the consumer group id
    * @param ts
    *   the target [[NJTimestamp]]
    * @return
    *   an effect that resets offsets
    */
  def resetOffsetsForTimes(groupId: String, ts: NJTimestamp): F[Unit]

  /** Retrieve a record from a specific partition and offset.
    *
    * @param partition
    *   the partition index
    * @param offset
    *   the offset within the partition
    * @return
    *   an effect that returns the record if it exists
    */
  def retrieveRecord(partition: Int, offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]]
}

object KafkaAdminApi {

  def apply[F[_]: Async](
    adminResource: Resource[F, KafkaAdminClient[F]],
    topicName: TopicName,
    consumerSettings: KafkaConsumerSettings): Resource[F, KafkaAdminApi[F]] =
    adminResource.map { client =>
      new KafkaTopicAdminApiImpl(client, topicName, consumerSettings)
    }

  final private class KafkaTopicAdminApiImpl[F[_]: Async](
    client: KafkaAdminClient[F],
    topicName: TopicName,
    consumerSettings: KafkaConsumerSettings)
      extends KafkaAdminApi[F] {

    override def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit] =
      client.deleteTopic(topicName.name.value)

    override def newTopic(numPartition: Int, numReplica: Short): F[Unit] =
      client.createTopic(new NewTopic(topicName.name.value, numPartition, numReplica))

    override def newTopic(description: TopicDescription): F[Unit] = {
      val partitions = description.partitions().size()
      val replicas = description.partitions().get(0).replicas().size().toShort
      newTopic(partitions, replicas)
    }

    override def mirrorTo(other: TopicName): F[Unit] =
      for {
        desc <- client.describeTopics(List(topicName.name.value)).map(_(topicName.name.value))
        _ <- client.createTopic(
          new NewTopic(
            other.name.value,
            desc.partitions().size(),
            desc.partitions().get(0).replicas().size().toShort))
      } yield ()

    override def describe: F[Map[String, TopicDescription]] =
      client.describeTopics(List(topicName.name.value))

    override def groups: F[List[GroupId]] =
      for {
        gIds <- client.listConsumerGroups.groupIds
        ids <- gIds.traverseFilter(gid =>
          client
            .listConsumerGroupOffsets(gid)
            .partitionsToOffsetAndMetadata
            .map(m => if (m.keySet.map(_.topic()).contains(topicName.name.value)) Some(gid) else None))
      } yield ids.distinct.map(GroupId(_))

    // consumer

    private val initCS: PureConsumerSettings =
      PureConsumerSettings.withProperties(consumerSettings.properties)

    private def transientConsumer(cs: PureConsumerSettings): TransientConsumer[F] =
      TransientConsumer(topicName, cs.withAutoOffsetReset(AutoOffsetReset.None).withEnableAutoCommit(false))

    override def lagBehind(groupId: String): F[TopicPartitionMap[Option[LagBehind]]] =
      for {
        ends <- transientConsumer(initCS.withGroupId(groupId)).endOffsets
        curr <- client
          .listConsumerGroupOffsets(groupId)
          .partitionsToOffsetAndMetadata
          .map(_.filter(_._1.topic() === topicName.name.value).map { case (k, v) => k -> Offset(v) })
          .map(TopicPartitionMap(_))
      } yield calculate.admin_lagBehind(ends, curr)

    override def deleteConsumerGroupOffsets(groupId: String): F[Unit] =
      for {
        tps <- transientConsumer(initCS.withGroupId(groupId)).partitionsFor
        _ <- client.deleteConsumerGroupOffsets(groupId, tps.value.toSet)
      } yield ()

    override def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]] =
      transientConsumer(initCS).offsetRangeFor(dtr)

    override def partitionsFor: F[ListOfTopicPartitions] =
      transientConsumer(initCS).partitionsFor

    override def commitSync(groupId: String, offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).commitSync(offsets)

    override def commitSync(groupId: String, partition: Int, offset: Long): F[Unit] = {
      val tp = new TopicPartition(topicName.name.value, partition)
      val oam = new OffsetAndMetadata(offset)
      commitSync(groupId, Map(tp -> oam))
    }

    override def resetOffsetsToBegin(groupId: String): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsToBegin

    override def resetOffsetsToEnd(groupId: String): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsToEnd

    override def resetOffsetsForTimes(groupId: String, ts: NJTimestamp): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsForTimes(ts)

    override def retrieveRecord(
      partition: Int,
      offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      transientConsumer(initCS).retrieveRecord(Partition(partition), Offset(offset))
  }
}
