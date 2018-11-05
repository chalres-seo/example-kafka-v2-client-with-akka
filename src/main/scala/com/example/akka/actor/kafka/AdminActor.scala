package com.example.akka.actor.kafka

import java.util.Properties

import akka.actor
import akka.actor.{Actor, ActorLogging, Props}
import com.example.kafka.admin.AdminClient
import com.example.utils.AppConfig
import com.example.akka.actor.kafka.AdminActor._
import akka.pattern.ask

import scala.concurrent.Future
import scala.collection.JavaConversions._

object AdminActor {
  def props = akka.actor.Props(new AdminActor(AppConfig.getKafkaAdminProps))
  def props(props:Properties): Props = akka.actor.Props(new AdminActor(props))

  final case class CreateTopic(topicName: String, partitionCount: Int, replicationFactor: Short)
  final case class DeleteTopic(topicName: String)

//  final case class DeleteAllTopic()

  final case class RequestExistTopic(requestId: Long, topicName: String)
  final case class ResponseExistTopic(requestId: Long, result: Boolean)

  final case class RequestTopicList(requestId: Long)
  final case class ResponseTopicList(requestId: Long, topicNameList: Set[String])
}

class AdminActor(props: Properties) extends Actor with ActorLogging {
  private var adminClient: AdminClient = _

  override def preStart(): Unit = {
    log.info("kafka admin client start")
    adminClient = AdminClient(AppConfig.getKafkaAdminProps)
  }
  override def postStop(): Unit = {
    log.info("kafka admin client stop")
    adminClient.close()
  }

  override def receive: Receive = {
    case CreateTopic(topicName, partitionCount, replicationFactor) =>
      log.info(s"create topic. name: $topicName, partition count: $partitionCount, replication factor: $replicationFactor")
      adminClient.createTopic(topicName, partitionCount, replicationFactor)

    case DeleteTopic(topicName) =>
      log.info(s"delete topic. name: $topicName")
      adminClient.deleteTopic(topicName)

    case RequestExistTopic(requestId, topicName) =>
      log.info(s"check topic exist. name: $topicName")
      sender() ! ResponseExistTopic(requestId, adminClient.isExistTopic(topicName))

    case RequestTopicList(requestId) =>
      log.info("request topic list.")
      sender() ! ResponseTopicList(requestId, adminClient.getTopicNameList.get.toSet)
  }
}
