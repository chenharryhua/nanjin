package com.github.chenharryhua.nanjin

import cats.Monad
import com.github.chenharryhua.nanjin.kafka.KafkaTopic

package object sparkafka {

  implicit class TopicDatasetSynax[F[_], K, V](val topic: KafkaTopic[F, K, V]) extends AnyVal {
    def topicDataset(implicit ev: Monad[F]): TopicDataset[F, K, V] = TopicDataset(topic)
  }
}
