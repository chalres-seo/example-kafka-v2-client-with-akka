package com.example.akka.actor.kafka

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.example.akka.actor.kafka.AdminActor.ResponseTopicList
import com.example.kafka.admin.Admin
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.example.akka.actor.kafka.AdminActor._

class TestAdminClientActor {
  private val testTopicName = TestAdminClientActor.testTopicName
  private val testTopicPartitionCount = TestAdminClientActor.testTopicPartitionCount
  private val testTopicReplicationFactor:Short = TestAdminClientActor.testTopicReplicationFactor


  private var testKafkaAdmin: Admin = TestAdminClientActor.testKafkaAdmin

  private var testActorSystem: ActorSystem = TestAdminClientActor.testActorSystem
  private var testActorProbe: TestProbe = TestAdminClientActor.testActorProbe

  private var testAdminClientActor: ActorRef = TestAdminClientActor.testAdminClientActor

  @Test
  def testCreateAndDeleteWithTopicListWithExistTopic(): Unit = {
    val testRequestId = 1
    val testTopicName = "test-create-and-delete-topic-actor"
    val testTopicPartitionCount = 1
    val testTopicReplicationFactor: Short = 1

    testAdminClientActor.tell(AdminActor.RequestTopicList(testRequestId), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseTopicList].topicNameList.contains(testTopicName), is(false))

    testAdminClientActor.tell(AdminActor.RequestExistTopic(testRequestId, testTopicName), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseExistTopic].result, is(false))

    testAdminClientActor
      .tell(AdminActor.CreateTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor), Actor.noSender)
    testActorProbe.expectNoMessage()

    testAdminClientActor.tell(AdminActor.RequestTopicList(testRequestId), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseTopicList].topicNameList.contains(testTopicName), is(true))

    testAdminClientActor.tell(AdminActor.RequestExistTopic(testRequestId, testTopicName), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseExistTopic].result, is(true))

    testAdminClientActor
      .tell(AdminActor.DeleteTopic(testTopicName), Actor.noSender)
    testActorProbe.expectNoMessage()
  }
}

private[this] object TestAdminClientActor {
  private val testTopicName = "test-kafka-admin-client-actor"
  private val testTopicPartitionCount = 3
  private val testTopicReplicationFactor:Short = 3

  private var testKafkaAdmin: Admin = _

  private var testActorSystem: ActorSystem = _
  private var testActorProbe: TestProbe = _
  private var testAdminClientActor: ActorRef = _

  @BeforeClass
  def beforeClass(): Unit = {
    testKafkaAdmin = Admin()

    testActorSystem = ActorSystem.create("test-admin-client-actor")
    testActorProbe = TestProbe()(testActorSystem)
    testAdminClientActor = testActorSystem.actorOf(AdminActor.props)

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

