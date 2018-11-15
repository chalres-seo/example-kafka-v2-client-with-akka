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

    testAdminActor.tell(AdminActor.CreateTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor), Actor.noSender)
    testActorProbe.expectNoMessage()

    testAdminActor.tell(AdminActor.RequestTopicList(testRequestId), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseTopicList].topicNameList.contains(testTopicName), is(true))

    testAdminActor.tell(AdminActor.RequestExistTopic(testRequestId, testTopicName), testActorProbe.ref)
    Assert.assertThat(testActorProbe.expectMsgType[ResponseExistTopic].result, is(true))

    testAdminActor.tell(AdminActor.DeleteTopic(testTopicName), Actor.noSender)
    testActorProbe.expectNoMessage()
  }
}

private object TestAdminActor {
  val testActorSystem: ActorSystem = ActorSystem.create("test-admin-client-actor")
  val testActorProbe: TestProbe = TestProbe()(testActorSystem)

  var testAdminActor: ActorRef = testActorSystem.actorOf(AdminActor.props(AppConfig.DEFAULT_KAFKA_ADMIN_PROPS))

  @BeforeClass
  def beforeClass(): Unit = {
    //nothing to do before unit test
  }

  @AfterClass
  def tearDownClass(): Unit = {
    Await.result(testActorSystem.terminate(), Duration.Inf)
  }
}

