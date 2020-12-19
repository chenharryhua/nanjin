package example

import example.database.DatabaseExample
import example.kafka.KafkaBasic
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new KafkaBasic,
      new DatabaseExample
    )
