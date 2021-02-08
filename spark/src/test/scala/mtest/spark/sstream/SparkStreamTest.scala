package mtest.spark.sstream

import org.scalatest.Sequential

class SparkStreamTest extends Sequential(new SparkKafkaStreamTest, new SparkStreamJoinTest, new SparkDStreamTest)
