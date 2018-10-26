package com.example.kafka.admin

import com.typesafe.scalalogging.LazyLogging
import org.junit._
import org.hamcrest.CoreMatchers._
import org.junit.rules.ExpectedException

import scala.annotation.meta.getter


import com.example.kafka.admin.TestKafkaAdmin._

class TestKafkaAdmin extends LazyLogging {
  @(Rule @getter)
  val exceptions: ExpectedException = rules.ExpectedException.none

  @Test
  def testCreateAndDeleteWithListAndExistTopic(): Unit = {
    val testCreateDeleteTopicName = "test-create-and-delete"

    testKafkaAdmin.deleteTopic(testCreateDeleteTopicName).get
    Assert.assertThat(testKafkaAdmin.isExistTopic(testCreateDeleteTopicName), is(false))
    testKafkaAdmin.createTopic(testCreateDeleteTopicName, 1, 1).get
    Assert.assertThat(testKafkaAdmin.isExistTopic(testCreateDeleteTopicName), is(true))
    Assert.assertThat(testKafkaAdmin.getTopicNameList.contains(testCreateDeleteTopicName), is(true))

    testKafkaAdmin.deleteTopic(testCreateDeleteTopicName).get
    Assert.assertThat(testKafkaAdmin.isExistTopic(testCreateDeleteTopicName), is(false))
    Assert.assertThat(testKafkaAdmin.getTopicNameList.contains(testCreateDeleteTopicName), is(false))
  }
}

object TestKafkaAdmin {
  val testTopicName = "test-kafka-admin-client"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor:Short = 3

  var testKafkaAdmin: Admin = _

  @BeforeClass
  def beforeClass(): Unit = {
    testKafkaAdmin = Admin()

    testKafkaAdmin.deleteTopic(testTopicName).get
    testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
  }

  @AfterClass
  def tearDownClass(): Unit = {
    testKafkaAdmin.deleteTopic(testTopicName).get
    testKafkaAdmin.close()
  }
}
