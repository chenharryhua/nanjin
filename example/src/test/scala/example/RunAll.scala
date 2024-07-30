package example

import example.database.ExampleDatabase
import example.kafka.{ExampleKafkaBasic, ExampleKafkaKStream}
import example.spark.ExampleKafkaDump
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new ExampleKafkaBasic,
      new ExampleKafkaKStream,
      new ExampleDatabase,
      new ExampleKafkaDump
    )
