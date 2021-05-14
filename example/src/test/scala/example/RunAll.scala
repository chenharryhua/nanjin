package example

import example.database.DatabaseExample
import example.kafka.{KafkaBasic, KafkaKStream}
import example.spark.{KafakDump, KafkaDirectStream, KafkaStructuredStream}
import org.scalatest.Sequential

class RunAll
    extends Sequential(
      new KafkaBasic,
      new KafkaKStream,
      new DatabaseExample,
      new KafkaDirectStream,
      new KafkaStructuredStream,
      new KafakDump
    )
