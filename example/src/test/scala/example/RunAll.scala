package example

import example.database.ExampleDatabase
import example.kafka.{ExampleKafkaBasic, ExampleKafkaKStream}
import example.spark.{ExampleKafakDump, ExampleKafkaDirectStream, ExampleKafkaStructuredStream}
import org.scalatest.{Ignore, Sequential}

@Ignore
class RunAll
    extends Sequential(
      new ExampleKafkaBasic,
      new ExampleKafkaKStream,
      new ExampleDatabase,
      new ExampleKafkaDirectStream,
      new ExampleKafkaStructuredStream,
      new ExampleKafakDump
    )
