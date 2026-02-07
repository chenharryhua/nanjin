package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import fs2.kafka.KafkaAdminClient
import org.apache.kafka.clients.admin.{NewTopic, TopicDescription}
import org.apache.kafka.clients.consumer.ConsumerRecord

trait AdminTopic[F[_]] {

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

  /** Create a new topic based on the given `TopicDescription`.
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

  /** Describe the topic associated with this admin instance.
    *
    * @return
    *   a map of topic name to `TopicDescription`; in practice, this will contain a single entry for the topic
    */
  def describe: F[Map[String, TopicDescription]]

  /** List all consumer groups consuming this topic.
    *
    * @return
    *   a list of `GroupId` for all consumer groups that have offsets in this topic
    */
  def groups: F[List[GroupId]]

  /** List all partitions of this topic.
    *
    * @return
    *   a `ListOfTopicPartitions` containing partition information
    */
  def partitionsFor: F[ListOfTopicPartitions]

  /** Compute offset ranges for the topic for a given time window.
    *
    * @param dtr
    *   the `DateTimeRange` for which to compute offsets
    * @return
    *   a map of topic partitions to optional offset ranges
    */
  def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]]

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

final private class AdminTopicImpl[F[_]: Sync](
  adminClient: KafkaAdminClient[F],
  consumerClient: SnapshotConsumer[F],
  topicName: TopicName)
    extends AdminTopic[F] {
  override def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit] =
    adminClient.deleteTopic(topicName.name.value)

  override def newTopic(numPartition: Int, numReplica: Short): F[Unit] =
    adminClient.createTopic(new NewTopic(topicName.name.value, numPartition, numReplica))

  override def newTopic(description: TopicDescription): F[Unit] = {
    val partitions = description.partitions().size()
    val replicas = description.partitions().get(0).replicas().size().toShort
    newTopic(partitions, replicas)
  }

  override def mirrorTo(other: TopicName): F[Unit] =
    for {
      desc <- adminClient.describeTopics(List(topicName.name.value)).map(_(topicName.name.value))
      _ <- adminClient.createTopic(
        new NewTopic(
          other.name.value,
          desc.partitions().size(),
          desc.partitions().get(0).replicas().size().toShort))
    } yield ()

  override def describe: F[Map[String, TopicDescription]] =
    adminClient.describeTopics(List(topicName.name.value))

  override def groups: F[List[GroupId]] =
    for {
      gIds <- adminClient.listConsumerGroups.groupIds
      ids <- gIds.traverseFilter(gid =>
        adminClient
          .listConsumerGroupOffsets(gid)
          .partitionsToOffsetAndMetadata
          .map(m => if (m.keySet.map(_.topic()).contains(topicName.name.value)) Some(gid) else None))
    } yield ids.distinct.map(GroupId(_))

  override def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]] =
    consumerClient.offsetRangeFor(dtr)

  override def partitionsFor: F[ListOfTopicPartitions] =
    consumerClient.partitionsFor

  override def retrieveRecord(
    partition: Int,
    offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    consumerClient.retrieveRecord(Partition(partition), Offset(offset))

}
