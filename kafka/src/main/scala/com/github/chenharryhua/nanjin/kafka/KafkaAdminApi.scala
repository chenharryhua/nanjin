package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import cats.{Endo, Id}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import fs2.kafka.{AdminClientSettings, AutoOffsetReset, ConsumerSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.{NewTopic, TopicDescription}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaAdminApi[F[_]] extends UpdateConfig[AdminClientSettings, KafkaAdminApi[F]] {
  def adminResource: Resource[F, KafkaAdminClient[F]]

  def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit]
  def describe: F[Map[String, TopicDescription]]
  def groups: F[List[KafkaConsumerGroupInfo]]

  def newTopic(numPartition: Int, numReplica: Short): F[Unit]
  def mirrorTo(other: TopicName, numReplica: Short): F[Unit]

  def deleteConsumerGroupOffsets(groupId: String): F[Unit]

  def partitionsFor: F[ListOfTopicPartitions]
  def offsetRangeFor(dtr: NJDateTimeRange): F[KafkaTopicPartition[Option[KafkaOffsetRange]]]

  def commitSync(groupId: String, offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def resetOffsetsToBegin(groupId: String): F[Unit]
  def resetOffsetsToEnd(groupId: String): F[Unit]
  def resetOffsetsForTimes(groupId: String, ts: NJTimestamp): F[Unit]
}

object KafkaAdminApi {

  def apply[F[_]: Async](
    topicName: TopicName,
    consumerSettings: KafkaConsumerSettings,
    adminSettings: AdminClientSettings): KafkaAdminApi[F] =
    new KafkaTopicAdminApiImpl(topicName, consumerSettings, adminSettings)

  final private class KafkaTopicAdminApiImpl[F[_]: Async](
    topicName: TopicName,
    consumerSettings: KafkaConsumerSettings,
    adminSettings: AdminClientSettings)
      extends KafkaAdminApi[F] {

    override val adminResource: Resource[F, KafkaAdminClient[F]] =
      KafkaAdminClient.resource[F](adminSettings)

    override def iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Unit] =
      adminResource.use(_.deleteTopic(topicName.value))

    override def newTopic(numPartition: Int, numReplica: Short): F[Unit] =
      adminResource.use(_.createTopic(new NewTopic(topicName.value, numPartition, numReplica)))

    override def mirrorTo(other: TopicName, numReplica: Short): F[Unit] =
      adminResource.use { c =>
        for {
          desc <- c.describeTopics(List(topicName.value)).map(_(topicName.value))
          _ <- c.createTopic(new NewTopic(other.value, desc.partitions().size(), numReplica))
        } yield ()
      }

    override def describe: F[Map[String, TopicDescription]] =
      adminResource.use(_.describeTopics(List(topicName.value)))

    // consumer
    import TransientConsumer.PureConsumerSettings

    private val initCS: PureConsumerSettings = {
      val pcs: PureConsumerSettings = ConsumerSettings[Id, Nothing, Nothing](null, null)
      pcs.withProperties(consumerSettings.properties)
    }

    private def transientConsumer(cs: PureConsumerSettings): TransientConsumer[F] =
      TransientConsumer(topicName, cs.withAutoOffsetReset(AutoOffsetReset.None).withEnableAutoCommit(false))

    override def groups: F[List[KafkaConsumerGroupInfo]] =
      adminResource.use { client =>
        for {
          uuid <- UUIDGen[F].randomUUID
          end <- transientConsumer(initCS.withGroupId(uuid.show)).endOffsets
          gIds <- client.listConsumerGroups.groupIds
          all <- gIds.traverse(gid =>
            client
              .listConsumerGroupOffsets(gid)
              .partitionsToOffsetAndMetadata
              .map(m => KafkaConsumerGroupInfo(gid, end, m)))
        } yield all.filter(_.lag.nonEmpty)
      }

    override def deleteConsumerGroupOffsets(groupId: String): F[Unit] =
      for {
        uuid <- UUIDGen[F].randomUUID
        tps <- transientConsumer(initCS.withGroupId(uuid.show)).partitionsFor
        _ <- adminResource.use(_.deleteConsumerGroupOffsets(groupId, tps.value.toSet))
      } yield ()

    override def offsetRangeFor(dtr: NJDateTimeRange): F[KafkaTopicPartition[Option[KafkaOffsetRange]]] =
      for {
        uuid <- UUIDGen[F].randomUUID
        kor <- transientConsumer(initCS.withGroupId(uuid.show)).offsetRangeFor(dtr)
      } yield kor

    override def partitionsFor: F[ListOfTopicPartitions] =
      for {
        uuid <- UUIDGen[F].randomUUID
        kor <- transientConsumer(initCS.withGroupId(uuid.show)).partitionsFor
      } yield kor

    override def commitSync(groupId: String, offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).commitSync(offsets)

    override def resetOffsetsToBegin(groupId: String): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsToBegin

    override def resetOffsetsToEnd(groupId: String): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsToEnd

    override def resetOffsetsForTimes(groupId: String, ts: NJTimestamp): F[Unit] =
      transientConsumer(initCS.withGroupId(groupId)).resetOffsetsForTimes(ts)

    override def updateConfig(f: Endo[AdminClientSettings]): KafkaAdminApi[F] =
      new KafkaTopicAdminApiImpl[F](topicName, consumerSettings, f(adminSettings))
  }
}
