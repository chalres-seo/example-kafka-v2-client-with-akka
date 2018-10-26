package com.example.kafka.producer

import com.example.kafka.admin.Admin
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit._
import org.hamcrest.CoreMatchers._
import com.example.kafka.producer.TestKafkaProducer._

class TestKafkaProducer {

  @Test
  def testProduceRecord() = {
    val result: RecordMetadata = testKafkaProducer.produceRecord(testProduceRecordSet(0)).get()

    println(Producer.recordMetadataToMap(result))
  }

  @Test
  def testProduceRecords() = {
    val result: Vector[RecordMetadata] = testKafkaProducer.produceRecords(testProduceRecordSet).map(_.get)

    result.foreach(r => println(Producer.recordMetadataToMap(r)))
  }
}

object TestKafkaProducer {
  val testTopicName = "test-kafka-producer"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testKafkaAdmin = Admin()
  var testKafkaProducer: Producer[String, String] = _

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[String, String]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i", s"value-$i")
    }.toVector


  @BeforeClass
  def beforeClass(): Unit = {
    testKafkaProducer = Producer(new StringSerializer, new StringSerializer)

    while(!testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      Thread.sleep(500)
    }
  }

  @AfterClass
  def tearDownClass(): Unit = {
    testKafkaProducer.close()

    while(testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.deleteTopic(testTopicName).get
      Thread.sleep(500)
    }

    testKafkaAdmin.close()
  }
}