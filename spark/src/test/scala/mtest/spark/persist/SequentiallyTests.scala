package mtest.spark.persist

import org.scalatest.Sequential

class SequentiallyTests
    extends Sequential(
      new AvroTest,
      new ParquetTest,
      new CirceTest,
      new JacksonTest,
      new JsonTest,
      new CsvTest
    )
