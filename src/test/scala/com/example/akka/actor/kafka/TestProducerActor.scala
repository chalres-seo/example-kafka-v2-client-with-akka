package com.example.akka.actor.kafka

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.example.akka.actor.kafka.ProducerActor._
import com.example.kafka.admin.AdminClient
import com.example.utils.AppConfig
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.example.akka.actor.kafka.TestProducerActor._
import com.example.kafka.producer.ProducerClient
import org.apache.kafka.clients.producer.ProducerRecord

class TestProducerActor {
  private val testRequestId = 1

  @Test
  def testProduceRecord(): Unit = {
    testProducerActor.tell(RequestProduceRecord(testRequestId, testProduceRecordSet(0)), testActorProbe.ref)
    val result = testActorProbe.expectMsgType[ResponseProduceRecord].recordMetadata.get

    Assert.assertThat(result.hasOffset, is(true))
    Assert.assertThat(result.hasTimestamp, is(true))
    Assert.assertThat(result.offset(), is(0L))
    Assert.assertThat(result.partition(), is(0))
    Assert.assertThat(result.topic(), is(testTopicName))
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

  var testKafkaAdmin: AdminClient = _

  var testActorSystem: ActorSystem = _
  var testActorProbe: TestProbe = _

  var testAdminActor: ActorRef = _
  var testProducerActor: ActorRef = _

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  @BeforeClass
  def beforeClass(): Unit = {
    testKafkaAdmin = AdminClient(AppConfig.getKafkaAdminProps)

    testActorSystem = ActorSystem.create("test-producer-client-actor")
    testActorProbe = TestProbe()(testActorSystem)

    testAdminActor = testActorSystem.actorOf(AdminActor.props)
    testProducerActor = testActorSystem.actorOf(ProducerActor.props)

    this.deleteTestTopic()
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {
    this.deleteTestTopic()

    testKafkaAdmin.close()
    Await.result(testActorSystem.terminate(), Duration.Inf)
  }

  def deleteTestTopic(): Unit = {
    testKafkaAdmin.deleteTopic(testTopicName).get
    while (testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.deleteTopic(testTopicName)
      Thread.sleep(500)
    }
  }

  def createTestTopic(): Unit = {
    testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
    while (!testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      Thread.sleep(500)
    }
  }
}