package mtest.kafka.stream

import org.scalatest.Sequential

class StreamTest extends Sequential(new KafkaStateStoreTest, new KafkaStreamingTest)
