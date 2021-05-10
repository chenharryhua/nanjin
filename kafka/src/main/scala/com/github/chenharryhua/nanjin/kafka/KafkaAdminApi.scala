package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.{NewTopic, TopicDescription}

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaAdminApi[F[_]] {
  val adminResource: Resource[F, KafkaAdminClient[F]]

  def idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Either[Throwable, Unit]]
  def describe: F[Map[String, TopicDescription]]
  def groups: F[List[KafkaConsumerGroupInfo]]

  def newTopic(numPartition: Int, numReplica: Short): F[Unit]
  def mirrorTo(other: TopicName, numReplica: Short): F[Unit]
}

object KafkaAdminApi {

  def apply[F[_]: Concurrent: ContextShift, K, V](topic: KafkaTopic[F, K, V]): KafkaAdminApi[F] =
    new KafkaTopicAdminApiImpl(topic)

  final private class KafkaTopicAdminApiImpl[F[_]: Concurrent: ContextShift, K, V](topic: KafkaTopic[F, K, V])
      extends KafkaAdminApi[F] {

    override val adminResource: Resource[F, KafkaAdminClient[F]] =
      KafkaAdminClient.resource[F](AdminClientSettings[F].withProperties(topic.context.settings.adminSettings.config)) 

    override def idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence: F[Either[Throwable, Unit]] =
      adminResource.use(_.deleteTopic(topic.topicName.value).attempt)

    override def newTopic(numPartition: Int, numReplica: Short): F[Unit] =
      adminResource.use(_.createTopic(new NewTopic(topic.topicName.value, numPartition, numReplica)))

    override def mirrorTo(other: TopicName, numReplica: Short): F[Unit] =
      adminResource.use { c =>
        for {
          desc <- c.describeTopics(List(topic.topicName.value)).map(_(topic.topicName.value))
          _ <- c.createTopic(new NewTopic(other.value, desc.partitions().size(), numReplica))
        } yield ()
      }

    override def describe: F[Map[String, TopicDescription]] =
      adminResource.use(_.describeTopics(List(topic.topicName.value)))

    override def groups: F[List[KafkaConsumerGroupInfo]] =
      adminResource.use { client =>
        for {
          end <- topic.shortLiveConsumer.use(_.endOffsets)
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
