package example

import example.database.ExampleDatabase
import example.kafka.{ExampleKafkaBasic, ExampleKafkaDump, ExampleKafkaKStream}
import org.scalatest.Sequential

class RunAll extends Sequential(
      new ExampleKafkaBasic,
      new ExampleKafkaKStream,
      new ExampleDatabase,
      new ExampleKafkaDump
    )
