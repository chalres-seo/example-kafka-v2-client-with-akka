package com.example.akka.actor.kafka

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.example.akka.actor.kafka.ProducerActor._
import com.example.kafka.admin.AdminClient
import com.example.utils.AppConfig
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.example.akka.actor.kafka.TestProducerActor._
import org.apache.kafka.clients.producer.ProducerRecord

class TestProducerActor {
  private val testRequestId = 1

  @Test
  def testProduceRecord(): Unit = {
    testProducerActor.tell(RequestProduceRecords(testRequestId, Vector(testProduceRecordSet(0))), testActorProbe.ref)
    val result = testActorProbe.expectMsgType[ResponseProduceRecords[Any, Any]]

    Assert.assertThat(result.remainedRecords.length, is(0))
    Assert.assertThat(result.recordsMetadata.length, is(1))
    Assert.assertThat(result.recordsMetadata.count(_.topic() == testTopicName), is(1))

    val resultRecord = result.recordsMetadata.head

    Assert.assertThat(resultRecord.hasOffset, is(true))
    Assert.assertThat(resultRecord.hasTimestamp, is(true))
    Assert.assertThat(resultRecord.offset(), is(0L))
    Assert.assertThat(resultRecord.partition(), is(0))
    Assert.assertThat(resultRecord.topic(), is(testTopicName))
  }

  @Test
  def testProduceRecords(): Unit = {
    testProducerActor.tell(RequestProduceRecords(testRequestId, testProduceRecordSet), testActorProbe.ref)
    val result = testActorProbe.expectMsgType[ResponseProduceRecords[Any, Any]]

    Assert.assertThat(result.remainedRecords.length, is(0))
    Assert.assertThat(result.recordsMetadata.length, is(testProduceRecordSetCount))
    Assert.assertThat(result.recordsMetadata.count(_.topic() == testTopicName), is(testProduceRecordSetCount))
  }
}

object TestProducerActor {
  val testTopicName = "test-kafka-producer-client-actor"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor:Short = 3

  val testAdminClient: AdminClient = AdminClient(AppConfig.createDefaultKafkaAdminProps)

  var testActorSystem: ActorSystem = ActorSystem.create("test-producer-client-actor")
  var testActorProbe: TestProbe = TestProbe()(testActorSystem)

  var testProducerActor: ActorRef = testActorSystem.actorOf(ProducerActor.props(AppConfig.DEFAULT_KAFKA_PRODUCER_PROPS))

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  @BeforeClass
  def beforeClass(): Unit = {
    this.deleteTestTopic()
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {
    this.deleteTestTopic()

    testAdminClient.close()
    Await.result(testActorSystem.terminate(), Duration.Inf)
  }

  def createTestTopic(): Unit = {
    if (!testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      while(!testAdminClient.isExistTopic(testTopicName)) {
        testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
        Thread.sleep(500)
      }
    }
  }

  def deleteTestTopic(): Unit = {
    if (testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.deleteTopic(testTopicName).get
      while(testAdminClient.isExistTopic(testTopicName)) {
        testAdminClient.deleteTopic(testTopicName).get
        Thread.sleep(500)
      }
    }
  }}