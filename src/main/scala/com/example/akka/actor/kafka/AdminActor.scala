package com.example.akka.actor.kafka

import java.util.Properties

import akka.actor
import akka.actor.{Actor, ActorLogging, Props}
import com.example.kafka.admin.AdminClient
import com.example.utils.AppConfig
import com.example.akka.actor.kafka.AdminActor._
import akka.pattern.ask
import org.apache.kafka.common.KafkaFuture

import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.util.Try

object AdminActor {
  def props(props:Properties): Props = akka.actor.Props(new AdminActor(props))

  final case class CreateTopic(topicName: String, partitionCount: Int, replicationFactor: Short)
  final case class DeleteTopic(topicName: String)

  final case class RequestExistTopic(requestId: Long, topicName: String)
  final case class ResponseExistTopic(requestId: Long, result: Boolean)

  final case class RequestTopicList(requestId: Long)
  final case class ResponseTopicList(requestId: Long, topicNameList: Set[String])
}

class AdminActor(props: Properties) extends Actor with ActorLogging {
  private var adminClient: AdminClient = _
  private var clientId: String = _

  def getClientId: String = clientId

  override def preStart(): Unit = {
    log.info("kafka admin actor start")
    adminClient = AdminClient(props)
    clientId = adminClient.getClientId
    log.info(s"kafka admin actor client id: $clientId")
  }

  override def postStop(): Unit = {
    log.info(s"kafka admin actor stop. client id: $clientId")
    adminClient.close()
  }

  override def receive: Receive = {
    case msg @ CreateTopic(topicName, partitionCount, replicationFactor) =>
      log.info(s"create topic. msg: $msg")
      Try(adminClient.createTopic(topicName, partitionCount, replicationFactor)).recover {
        case t: Throwable =>
          log.error(s"failed create topic. msg: $msg")
          log.error(t.getMessage, t)
          throw t
      }

    case msg @ DeleteTopic(topicName) =>
      log.info(s"delete topic. name: $topicName")
      Try(adminClient.deleteTopic(topicName)).recover {
        case t: Throwable =>
          log.error(s"failed delete topic. msg: $msg")
          log.error(t.getMessage, t)
          throw t
      }

    case msg @ RequestExistTopic(requestId, topicName) =>
      log.info(s"check topic exist. msg: $msg")
      sender() ! ResponseExistTopic(requestId, adminClient.isExistTopic(topicName))

    case msg @ RequestTopicList(requestId) =>
      log.info(s"request topic list. msg: $msg")
      sender() ! ResponseTopicList(requestId, adminClient.getTopicNameList.get.toSet)
  }
}
