package com.github.chenharryhua.nanjin

package object codec
    extends KafkaIsoInstances with BitraverseMessageInstances with BitraverseMessagesInstances {
  object bitraverse extends BitraverseMessageInstances with BitraverseMessagesInstances
  object iso extends KafkaIsoInstances
}
