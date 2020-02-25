package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.common.{KafkaConsumerGroupInfo, TopicName}
import fs2.kafka.{adminClientResource, AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.{NewTopic, TopicDescription}

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaTopicAdminApi[F[_]] {
  val adminResource: Resource[F, KafkaAdminClient[F]]

  def IdefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Either[Throwable, Unit]]
  def describe: F[Map[String, TopicDescription]]
  def groups: F[List[KafkaConsumerGroupInfo]]

  def newTopic(numPartition: Int, numReplica: Short): F[Unit]
  def mirrorTo(other: TopicName, numReplica: Short): F[Unit]
}

object KafkaTopicAdminApi {

  def apply[F[_]: Concurrent: ContextShift, K, V](kit: KafkaTopicKit[K, V]): KafkaTopicAdminApi[F] =
    new KafkaTopicAdminApiImpl(kit)

  final private class KafkaTopicAdminApiImpl[F[_]: Concurrent: ContextShift, K, V](
    kit: KafkaTopicKit[K, V])
      extends KafkaTopicAdminApi[F] {

    override val adminResource: Resource[F, KafkaAdminClient[F]] =
      adminClientResource[F](
        AdminClientSettings[F].withProperties(kit.settings.adminSettings.config))

    override def IdefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      : F[Either[Throwable, Unit]] =
      adminResource.use(_.deleteTopic(kit.topicName.value).attempt)

    override def newTopic(numPartition: Int, numReplica: Short): F[Unit] =
      adminResource.use(_.createTopic(new NewTopic(kit.topicName.value, numPartition, numReplica)))

    override def mirrorTo(other: TopicName, numReplica: Short): F[Unit] =
      adminResource.use { c =>
        for {
          desc <- c.describeTopics(List(kit.topicName.value)).map(_(kit.topicName.value))
          _ <- c.createTopic(new NewTopic(other.value, desc.partitions().size(), numReplica))
        } yield ()
      }

    override def describe: F[Map[String, TopicDescription]] =
      adminResource.use(_.describeTopics(List(kit.topicName.value)))

    override def groups: F[List[KafkaConsumerGroupInfo]] =
      adminResource.use { client =>
        for {
          end <- KafkaConsumerApi(kit).use(_.endOffsets)
          gids <- client.listConsumerGroups.groupIds
          all <- gids.traverse(gid =>
            client
              .listConsumerGroupOffsets(gid)
              .partitionsToOffsetAndMetadata
              .map(m => KafkaConsumerGroupInfo(gid, end, m)))
        } yield all.filter(_.lag.nonEmpty)
      }
  }
}
