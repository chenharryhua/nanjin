package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import cats.tagless.autoFunctorK
import fs2.kafka.{adminClientResource, AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final case class KafkaConsumerGroupOffsets(
  groupId: String,
  topicOffset: Map[TopicPartition, OffsetAndMetadata]) {

  def filter(topicName: String): Option[KafkaConsumerGroupOffsets] = {
    val nc = topicOffset.filter(tos => tos._1.topic() === topicName)
    if (nc.nonEmpty) Some(copy(topicOffset = nc)) else None
  }

  override def toString: String = show

  def show: String = {
    val to: String = topicOffset.map { case (t, v) => s"$t -> ${v.offset()}" }.mkString("\n")
    s"""
       |group id: $groupId
       |$to
       |
       |""".stripMargin
  }
}

// delegate to https://ovotech.github.io/fs2-kafka/
@autoFunctorK
sealed abstract class KafkaTopicAdminApi[F[_]] {
  def IdefinitelyWantDeleteTheTopic: F[Unit]
  def describe: F[Map[String, TopicDescription]]
  def consumerGroupOffsets: F[List[KafkaConsumerGroupOffsets]]
}

object KafkaTopicAdminApi {

  def apply[F[_]: Concurrent: ContextShift, K, V](
    topic: KafkaTopic[F, K, V],
    adminSettings: AdminClientSettings[F]
  ): KafkaTopicAdminApi[F] = new DelegateToFs2(topic, adminSettings)

  final private class DelegateToFs2[F[_]: Concurrent: ContextShift, K, V](
    topic: KafkaTopic[F, K, V],
    adminSettings: AdminClientSettings[F])
      extends KafkaTopicAdminApi[F] {

    private val admin: Resource[F, KafkaAdminClient[F]] =
      adminClientResource[F](adminSettings)

    override def IdefinitelyWantDeleteTheTopic: F[Unit] =
      admin.use(_.deleteTopic(topic.topicName))

    override def describe: F[Map[String, TopicDescription]] =
      admin.use(_.describeTopics(List(topic.topicName)))

    override def consumerGroupOffsets: F[List[KafkaConsumerGroupOffsets]] =
      admin.use { client =>
        for {
          gids <- client.listConsumerGroups.groupIds
          all <- gids.traverse(
            g =>
              client
                .listConsumerGroupOffsets(g)
                .partitionsToOffsetAndMetadata
                .map(m => KafkaConsumerGroupOffsets(g, m)))
        } yield all.flatMap(_.filter(topic.topicName))
      }
  }
}

object KafkaConsumerGroupOffsets {
  implicit val showKafkaConsumerGroupOffsets: Show[KafkaConsumerGroupOffsets] = _.show
}

@autoFunctorK
sealed abstract class KafkaAdminApi[F[_]] {
  def consumerGroups: F[List[String]]
  def listTopics: F[Set[String]]
  def listConsumerGroupOffsets: F[List[KafkaConsumerGroupOffsets]]
}

object KafkaAdminApi {

  def apply[F[_]: Concurrent: ContextShift](
    adminSettings: AdminClientSettings[F]): KafkaAdminApi[F] =
    new DelegateToFs2(adminSettings)

  final private class DelegateToFs2[F[_]: Concurrent: ContextShift](
    adminSettings: AdminClientSettings[F])
      extends KafkaAdminApi[F] {

    private val admin: Resource[F, KafkaAdminClient[F]] =
      adminClientResource[F](adminSettings)

    override def consumerGroups: F[List[String]] =
      admin.use(_.listConsumerGroups.groupIds)

    override def listTopics: F[Set[String]] =
      admin.use(_.listTopics.names)

    override def listConsumerGroupOffsets: F[List[KafkaConsumerGroupOffsets]] =
      admin.use { client =>
        consumerGroups.flatMap(
          _.traverse(
            g =>
              client
                .listConsumerGroupOffsets(g)
                .partitionsToOffsetAndMetadata
                .map(m => KafkaConsumerGroupOffsets(g, m))))
      }
  }
}
