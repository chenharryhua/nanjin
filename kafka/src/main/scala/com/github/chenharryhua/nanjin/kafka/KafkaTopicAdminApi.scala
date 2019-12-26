package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import cats.tagless.{autoFunctorK, autoSemigroupalK}
import fs2.kafka.{adminClientResource, AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.TopicDescription

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaTopicAdminApi[F[_]] {
  def idefinitelyWantDeleteTheTopic: F[Unit]
  def describe: F[Map[String, TopicDescription]]
  def groups: F[List[KafkaConsumerGroupInfo]]
}

object KafkaTopicAdminApi {

  def apply[F[_]: Concurrent: ContextShift, K, V](
    topic: KafkaTopic[F, K, V]
  ): KafkaTopicAdminApi[F] = new KafkaTopicAdminApiImpl(topic)

  final private class KafkaTopicAdminApiImpl[F[_]: Concurrent: ContextShift, K, V](
    topic: KafkaTopic[F, K, V])
      extends KafkaTopicAdminApi[F] {

    private val admin: Resource[F, KafkaAdminClient[F]] =
      adminClientResource[F](
        AdminClientSettings[F].withProperties(topic.context.settings.sharedAdminSettings.config))

    override def idefinitelyWantDeleteTheTopic: F[Unit] =
      admin.use(_.deleteTopic(topic.topicDef.topicName))

    override def describe: F[Map[String, TopicDescription]] =
      admin.use(_.describeTopics(List(topic.topicDef.topicName)))

    override def groups: F[List[KafkaConsumerGroupInfo]] =
      admin.use { client =>
        for {
          end <- topic.consumer.endOffsets
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
