package com.github.chenharryhua.nanjin

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic

package object sparkafka {

  implicit class TopicDatasetSyntax[F[_], K, V](val topic: KafkaTopic[F, K, V]) extends AnyVal {

    def topicDataset(implicit ev: ConcurrentEffect[F], ev2: Timer[F]): TopicDataset[F, K, V] =
      TopicDataset(topic)
  }
}
