package mtest.kafka.stream

import org.scalatest.Sequential

// sbt "kafka/testOnly mtest.kafka.stream.RunStreamingTest"

class RunStreamingTest extends Sequential(
      new InteractiveTest,
      new KafkaStreamingTest,
      new TransformerTest
    )
