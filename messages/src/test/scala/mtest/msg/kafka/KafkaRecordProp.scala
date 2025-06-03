package mtest.msg.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import fs2.kafka.{ConsumerRecord, ProducerRecord}
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.scalacheck.Properties

class KafkaRecordProp extends Properties("KafkaRecordProp") {
  import ArbitraryData.{
    abFs2ConsumerRecord,
    abFs2ProducerRecord,
    abKafkaConsumerRecord,
    abKafkaProducerRecord
  }
  import NJConsumerRecordTestData.*
  import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
  import org.scalacheck.Prop.forAll

  property("nj.producer.record.conversion") = forAll { (op: NJProducerRecord[Int, Int]) =>
    val fpr = op.toProducerRecord
    val jpr = op.toJavaProducerRecord

    NJProducerRecord(jpr) == NJProducerRecord(fpr)
  }

  property("fs2.producer.record.conversion") = forAll { (op: ProducerRecord[Int, Int]) =>
    val ncr = NJProducerRecord(op)
    val nc2 = NJProducerRecord(op.transformInto[JavaProducerRecord[Int, Int]])
    ncr == nc2
  }

  property("java.producer.record.conversion") = forAll { (op: JavaProducerRecord[Int, Int]) =>
    val ncr = NJProducerRecord(op)
    val nc2 = NJProducerRecord(op.transformInto[ProducerRecord[Int, Int]])
    ncr == nc2
  }

  property("nj.consumer.record.conversion") = forAll { (op: NJConsumerRecord[Int, Int]) =>
    val cr = op.toConsumerRecord
    val jcr = op.toJavaConsumerRecord
    NJConsumerRecord(cr) === op && NJConsumerRecord(jcr) === op
  }

  property("fs2.consumer.record.conversion") = forAll { (op: ConsumerRecord[Int, Int]) =>
    val ncr = NJConsumerRecord(op)
    val nc2 = NJConsumerRecord(op.transformInto[JavaConsumerRecord[Int, Int]])
    ncr === nc2
  }

  property("java.consumer.record.conversion") = forAll { (op: JavaConsumerRecord[Int, Int]) =>
    val ncr = NJConsumerRecord(op)
    val nc2 = NJConsumerRecord(op.transformInto[ConsumerRecord[Int, Int]])
    ncr === nc2
  }
}
