package example

import example.database.ExampleDatabase
import example.kafka.{ExampleKafkaBasic, ExampleKafkaKStream}
import example.spark.{ExampleKafakDump, ProblematicTest}
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new ExampleKafkaBasic,
      new ExampleKafkaKStream,
      new ExampleDatabase,
      new ExampleKafakDump
    )
