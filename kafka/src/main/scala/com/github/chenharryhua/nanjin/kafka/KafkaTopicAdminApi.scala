package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.common.KafkaConsumerGroupInfo
import fs2.kafka.{adminClientResource, AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.TopicDescription

// delegate to https://ovotech.github.io/fs2-kafka/

sealed trait KafkaTopicAdminApi[F[_]] {
  def idefinitelyWantToDeleteTheTopic: F[Either[Throwable, Unit]]
  def describe: F[Map[String, TopicDescription]]
  def groups: F[List[KafkaConsumerGroupInfo]]
  val adminResource: Resource[F, KafkaAdminClient[F]]
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

    override def idefinitelyWantToDeleteTheTopic: F[Either[Throwable, Unit]] =
      adminResource.use(_.deleteTopic(kit.topicName.value).attempt)

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
