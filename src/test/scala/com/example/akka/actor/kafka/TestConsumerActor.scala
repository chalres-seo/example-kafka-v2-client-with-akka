package com.example.akka.actor.kafka

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.example.kafka.admin.AdminClient
import com.example.utils.AppConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import TestConsumerActor._
import com.example.akka.actor.kafka.ConsumerActor._
import com.example.akka.actor.kafka.ProducerActor._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.annotation.tailrec
import scala.collection.JavaConversions._

class TestConsumerActor {
  private val testRequestId = 1

  @Test
  def testConsumeRecord(): Unit = {
    @tailrec
    def loop[K, V](consumerRecords: ConsumerRecords[K, V], consumedRecords: Iterator[ConsumerRecord[K, V]]): Iterator[ConsumerRecord[K, V]] = {
      if (consumerRecords.isEmpty) {
        consumedRecords
      } else {
        testConsumerActor.tell(ConsumerActor.RequestConsumerRecords(testRequestId), testActorProbe.ref)
        val nextConsumerRecords = testActorProbe.expectMsgType[ResponseConsumerRecords[K, V]](FiniteDuration(10L, TimeUnit.SECONDS)).consumeRecord
        loop(nextConsumerRecords, consumedRecords ++ consumerRecords.iterator())
      }
    }

    testConsumerActor.tell(ConsumerActor.RequestConsumerRecords(testRequestId), testActorProbe.ref)
    val prepareConsume = testActorProbe.expectMsgType[ResponseConsumerRecords[Any, Any]].consumeRecord
    val prepareConsumeRecords = loop(prepareConsume, Iterator.empty)

    Assert.assertThat(prepareConsumeRecords.length, is(0))

    testProducerActor.tell(ProducerActor.RequestProduceRecords(testRequestId, testProduceRecordSet), testActorProbe.ref)
    val produceResult = testActorProbe.expectMsgType[ResponseProduceRecords[Any, Any]]
    Assert.assertThat(produceResult.recordsMetadata.length, is(testProduceRecordSetCount))

    testConsumerActor.tell(ConsumerActor.RequestConsumerRecords(testRequestId), testActorProbe.ref)
    val consumerRecords = testActorProbe.expectMsgType[ResponseConsumerRecords[Any, Any]].consumeRecord
    val consumedRecords = loop(consumerRecords, Iterator.empty)

    Assert.assertThat(consumedRecords.length, is(testProduceRecordSetCount))
  }
}

object TestConsumerActor {
  val testTopicName = "test-kafka-consumer-client-actor"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor:Short = 3

  var testKafkaAdmin: AdminClient = _

  var testActorSystem: ActorSystem = _
  var testActorProbe: TestProbe = _

  var testProducerActor: ActorRef = _
  var testConsumerActor: ActorRef = _

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  @BeforeClass
  def beforeClass(): Unit = {
    testActorSystem = ActorSystem.create("test-producer-client-actor")
    testActorProbe = TestProbe()(testActorSystem)

    testKafkaAdmin = AdminClient(AppConfig.createDefaultKafkaAdminProps)

    testProducerActor = testActorSystem.actorOf(ProducerActor.props(AppConfig.DEFAULT_KAFKA_PRODUCER_PROPS))
    testConsumerActor = testActorSystem.actorOf(ConsumerActor.props(AppConfig.DEFAULT_KAFKA_CONSUMER_PROPS, testTopicName))

    this.deleteTestTopic()
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {
    Await.result(testActorSystem.terminate(), Duration.Inf)

    this.deleteTestTopic()
    testKafkaAdmin.close()
  }

  def deleteTestTopic(): Unit = {
    if (testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.deleteTopic(testTopicName).get
      while (testKafkaAdmin.isExistTopic(testTopicName)) {
        testKafkaAdmin.deleteTopic(testTopicName)
        Thread.sleep(500)
      }
    }
  }

  def createTestTopic(): Unit = {
    if (!testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      while (!testKafkaAdmin.isExistTopic(testTopicName)) {
        testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
        Thread.sleep(300)
      }
    }
  }
}
