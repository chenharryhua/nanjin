package com.github.chenharryhua.nanjin

package object codec {
  object bitraverse extends BitraverseMessageInstances with BitraverseMessagesInstances
  object iso extends KafkaIsoInstances
  object eq extends EqMessage
  object show extends ShowKafkaMessage
}
