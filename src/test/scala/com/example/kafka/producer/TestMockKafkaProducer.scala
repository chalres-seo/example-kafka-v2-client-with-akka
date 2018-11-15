package com.example.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.hamcrest.CoreMatchers._
import org.junit._

class TestMockKafkaProducer {
  val testTopicName = "test-mock-kafka-producer"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[String, String]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i", s"value-$i")
    }.toVector

  val mockKafkaProducer = new MockProducer(true, new StringSerializer, new StringSerializer)
  val mockProducerClient = ProducerClient(mockKafkaProducer, new Properties())

  @Test
  def testProduceRecords(): Unit = {
    mockProducerClient.produceRecords(testProduceRecordSet).foreach(_.get)

    Assert.assertThat(mockKafkaProducer.history().size(), is(testProduceRecordSetCount))
    mockKafkaProducer.history().clear()
  }
}