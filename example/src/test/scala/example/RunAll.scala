package example

import example.database.ExampleDatabase
import example.kafka.{ExampleKafkaBasic, ExampleKafkaKStream}
import example.spark.{ExampleKafakDump, ExampleKafkaStructuredStream}
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new ExampleKafkaBasic,
      new ExampleKafkaKStream,
      new ExampleDatabase,
      new ExampleKafkaStructuredStream,
      new ExampleKafakDump
    )
