package com.example.akka.actor.kafka

import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.example.akka.actor.kafka.ConsumerActor.{RequestConsumerRecords, RequestOffsetCommit, ResponseConsumerRecords}
import com.example.kafka.consumer.ConsumerClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecords

object ConsumerActor extends LazyLogging {
  def props(props:Properties, subscribeTopicName: String): Props = akka.actor.Props(new ConsumerActor(props, subscribeTopicName))

  case class RequestConsumerRecords[K, V](requestId: Long)
  case class ResponseConsumerRecords[K, V](requestId: Long, consumeRecord: ConsumerRecords[K, V])

  case class RequestOffsetCommit()
}

class ConsumerActor[K, V](props: Properties, subscribeTopicName: String) extends Actor with ActorLogging {
  private var consumerClient: ConsumerClient[K, V] = _

  private var groupId: String = _
  private var clientId: String = _

  def getGroupId: String = groupId
  def getClientId: String = clientId
  def getSubscribeTopicName: String = subscribeTopicName

  override def preStart(): Unit = {
    log.info("kafka consumer client start")
    consumerClient = ConsumerClient(props)
    groupId = consumerClient.getGroupId
    clientId = consumerClient.getClientId
    consumerClient.subscribeTopic(subscribeTopicName)
    log.info(s"kafka consumer group id: $groupId")
    log.info(s"kafka consumer client id: $clientId")
  }

  override def postStop(): Unit = {
    log.info(s"kafka consumer client stop. group id: $groupId, client id: $clientId")
    consumerClient.offsetCommit()
    consumerClient.unsubscribeAllTopic()
    consumerClient.close()
  }

  override def receive: Receive = {
    case RequestConsumerRecords(requestId) =>
      val consumerRecords: ConsumerRecords[K, V] = consumerClient.consumeRecord

      log.info(s"consume record count: ${consumerRecords.count()}")
      consumerClient.offsetCommitAsync()

      sender() ! ResponseConsumerRecords(requestId, consumerRecords)

    case RequestOffsetCommit() =>
      consumerClient.offsetCommitAsync()

  }
}
