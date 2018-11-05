package com.example.kafka.admin

import java.util.concurrent.ExecutionException

import com.typesafe.scalalogging.LazyLogging
import org.junit._
import org.hamcrest.CoreMatchers._
import org.junit.rules.ExpectedException

import scala.annotation.meta.getter
import com.example.kafka.admin.TestKafkaAdminClient._
import com.example.utils.AppConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

class TestKafkaAdminClient extends LazyLogging {
  @(Rule @getter)
  val exceptions: ExpectedException = rules.ExpectedException.none

  @Test
  def testCreateAndDeleteWithListAndExistTopic(): Unit = {
    val testCreateDeleteTopicName = "test-create-and-delete"

    testAdminClient.deleteTopic(testCreateDeleteTopicName).get
    Assert.assertThat(testAdminClient.isExistTopic(testCreateDeleteTopicName), is(false))
    testAdminClient.createTopic(testCreateDeleteTopicName, 1, 1).get
    Assert.assertThat(testAdminClient.isExistTopic(testCreateDeleteTopicName), is(true))
    Assert.assertThat(testAdminClient.getTopicNameList.get.contains(testCreateDeleteTopicName), is(true))

    testAdminClient.deleteTopic(testCreateDeleteTopicName).get
    Assert.assertThat(testAdminClient.isExistTopic(testCreateDeleteTopicName), is(false))
    Assert.assertThat(testAdminClient.getTopicNameList.get.contains(testCreateDeleteTopicName), is(false))
  }

  @Test
  def testUnknownTopicException(): Unit = {
    val testDescribeTopicName = "test-describe-topic"

    exceptions.expect(classOf[ExecutionException])
    exceptions.expectCause(isA(classOf[UnknownTopicOrPartitionException]))
    testAdminClient.describeTopic(testDescribeTopicName).get
  }
}

object TestKafkaAdminClient {
  val testTopicName = "test-kafka-admin-client"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor:Short = 3

  var testAdminClient: AdminClient = _

  @BeforeClass
  def beforeClass(): Unit = {
    testAdminClient = AdminClient(AppConfig.getKafkaAdminProps)

    testAdminClient.deleteTopic(testTopicName).get
    testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
  }

  @AfterClass
  def tearDownClass(): Unit = {
    testAdminClient.deleteTopic(testTopicName).get
    testAdminClient.close()
  }
}
