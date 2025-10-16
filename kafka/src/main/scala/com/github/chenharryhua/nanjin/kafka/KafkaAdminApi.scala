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
  def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit]
  def describe: F[Map[String, TopicDescription]]

  def groups: F[List[GroupId]]

  def lagBehind(groupId: String): F[TopicPartitionMap[Option[LagBehind]]]

  def newTopic(numPartition: Int, numReplica: Short): F[Unit]
  def mirrorTo(other: TopicName, numReplica: Short): F[Unit]

  def deleteConsumerGroupOffsets(groupId: String): F[Unit]

  def partitionsFor: F[ListOfTopicPartitions]
  def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]]

  def commitSync(groupId: String, offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def commitSync(groupId: String, partition: Int, offset: Long): F[Unit]
  def resetOffsetsToBegin(groupId: String): F[Unit]
  def resetOffsetsToEnd(groupId: String): F[Unit]
  def resetOffsetsForTimes(groupId: String, ts: NJTimestamp): F[Unit]

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

    override def mirrorTo(other: TopicName, numReplica: Short): F[Unit] =
      for {
        desc <- client.describeTopics(List(topicName.name.value)).map(_(topicName.name.value))
        _ <- client.createTopic(new NewTopic(other.name.value, desc.partitions().size(), numReplica))
      } yield ()

    override def describe: F[Map[String, TopicDescription]] =
      client.describeTopics(List(topicName.name.value))

    /** list of all consumer-groups which consume the topic
      * @return
      */
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
      pureConsumerSettings.withProperties(consumerSettings.properties)

    private def transientConsumer(cs: PureConsumerSettings): TransientConsumer[F] =
      TransientConsumer(topicName, cs.withAutoOffsetReset(AutoOffsetReset.None).withEnableAutoCommit(false))

    override def lagBehind(groupId: String): F[TopicPartitionMap[Option[LagBehind]]] =
      for {
        ends <- transientConsumer(initCS.withGroupId(groupId)).endOffsets
        curr <- client
          .listConsumerGroupOffsets(groupId)
          .partitionsToOffsetAndMetadata
          .map(_.filter(_._1.topic() === topicName.name.value).view.mapValues(Offset(_)).toMap)
          .map(TopicPartitionMap(_))
      } yield calculate.admin_lagBehind(ends, curr)

    /** remove consumer group from the topic
      * @param groupId
      *   consumer group id
      * @return
      */
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
