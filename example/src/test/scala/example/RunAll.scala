package example

import example.kafka.KafkaBasic
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new KafkaBasic
    )
