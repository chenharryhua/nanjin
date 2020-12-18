package example

import example.database.KafkaBasic
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new KafkaBasic
    )
