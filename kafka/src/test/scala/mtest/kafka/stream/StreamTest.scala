package mtest.kafka.stream

import org.scalatest.Sequential

class StreamTest extends Sequential(new KafkaStreamsBuildTest, new KafkaStreamingTest)
