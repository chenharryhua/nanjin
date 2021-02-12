package mtest.kafka.stream

import org.scalatest.Sequential

class StreamTest extends Sequential(new TransformerTest, new KafkaStreamingTest, new InteractiveTest)
