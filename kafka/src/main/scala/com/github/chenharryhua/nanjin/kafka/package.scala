package com.github.chenharryhua.nanjin

import cats.Id
import fs2.kafka.{ConsumerSettings, ProducerSettings}

package object kafka {
  type PureConsumerSettings = ConsumerSettings[Id, Nothing, Nothing]
  type PureProducerSettings = ProducerSettings[Id, Nothing, Nothing]

  val pureConsumerSettings: PureConsumerSettings = ConsumerSettings[Id, Nothing, Nothing](null, null)
  val pureProducerSettings: PureProducerSettings = ProducerSettings[Id, Nothing, Nothing](null, null)
}
