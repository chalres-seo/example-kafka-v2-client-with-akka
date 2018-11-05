package com.example.akka.actor.kafka

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.example.akka.actor.kafka.AdminActor.ResponseTopicList
import com.example.kafka.admin.AdminClient
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.example.akka.actor.kafka.AdminActor._
import com.example.utils.AppConfig

import com.example.akka.actor.kafka.TestAdminActor._

class TestAdminActor {
  @Test
  def testCreateAndDeleteWithTopicListWithExistTopic(): Unit = {
    val testRequestId = 1
    val testTopicName = "test-create-and-delete-topic-actor"
    val testTopicPartitionCount = 1
    val testTopicReplicationFactor: Short = 1

    testAdminActor.tell(AdminActor.RequestTopicList(testRequestId), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseTopicList].topicNameList.contains(testTopicName), is(false))

    testAdminActor.tell(AdminActor.RequestExistTopic(testRequestId, testTopicName), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseExistTopic].result, is(false))

    testAdminActor
      .tell(AdminActor.CreateTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor), Actor.noSender)
    testActorProbe.expectNoMessage()

    testAdminActor.tell(AdminActor.RequestTopicList(testRequestId), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseTopicList].topicNameList.contains(testTopicName), is(true))

    testAdminActor.tell(AdminActor.RequestExistTopic(testRequestId, testTopicName), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseExistTopic].result, is(true))

    testAdminActor
      .tell(AdminActor.DeleteTopic(testTopicName), Actor.noSender)
    testActorProbe.expectNoMessage()
  }
}

private object TestAdminActor {
  val testTopicName = "test-kafka-admin-client-actor"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor:Short = 3

  var testKafkaAdmin: AdminClient = _

  var testActorSystem: ActorSystem = _
  var testActorProbe: TestProbe = _

  var testAdminActor: ActorRef = _

  @BeforeClass
  def beforeClass(): Unit = {
    testKafkaAdmin = AdminClient(AppConfig.getKafkaAdminProps)

    testActorSystem = ActorSystem.create("test-admin-client-actor")
    testActorProbe = TestProbe()(testActorSystem)
    testAdminActor = testActorSystem.actorOf(AdminActor.props)

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

