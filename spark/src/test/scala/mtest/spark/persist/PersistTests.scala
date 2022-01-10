package mtest.spark.persist

import org.scalatest.Sequential

class PersistTests
    extends Sequential(
      new ConcurrencyTest,
      new AvroTest,
      new BinAvroTest,
      new ParquetTest,
      new CirceTest,
      new JacksonTest,
      new JsonTest,
      new CsvTest,
      new TextTest,
      new ObjectFileTest
    )
