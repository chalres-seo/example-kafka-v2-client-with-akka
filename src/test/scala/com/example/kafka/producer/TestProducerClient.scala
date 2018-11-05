package com.example.kafka.producer

import com.example.kafka.admin.AdminClient
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit._
import org.hamcrest.CoreMatchers._
import com.example.kafka.producer.TestProducerClient._
import com.example.utils.AppConfig

class TestProducerClient {

  @Test
  def testProduceRecord(): Unit = {
    val result: RecordMetadata = testProducerClient.produceRecord(testProduceRecordSet(0)).get()
    val resultMetaData: Map[String, Any] = ProducerClient.produceRecordMetadataToMap(result)

    Assert.assertThat(resultMetaData("topic").toString, is(testTopicName))
  }

  @Test
  def testProduceRecords(): Unit = {
    val result: Vector[RecordMetadata] = testProducerClient.produceRecords(testProduceRecordSet).map(_.get)
    val resultMetaData: Vector[Map[String, Any]] = result.map(ProducerClient.produceRecordMetadataToMap)

    Assert.assertThat(resultMetaData.length, is(testProduceRecordSetCount))
    Assert.assertThat(resultMetaData.count(_("topic").toString == testTopicName), is(testProduceRecordSetCount))
  }
}

object TestProducerClient {
  val testTopicName = "test-kafka-producer"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  val testAdminClient = AdminClient(AppConfig.getKafkaAdminProps)

  var testProducerClient: ProducerClient[Any, Any] = _


  @BeforeClass
  def beforeClass(): Unit = {
    testProducerClient = ProducerClient(AppConfig.getKafkaProducerProps)

    while(!testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      Thread.sleep(500)
    }
  }

  @AfterClass
  def tearDownClass(): Unit = {
    testProducerClient.close()

    while(testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.deleteTopic(testTopicName).get
      Thread.sleep(500)
    }

    testAdminClient.close()
  }
}