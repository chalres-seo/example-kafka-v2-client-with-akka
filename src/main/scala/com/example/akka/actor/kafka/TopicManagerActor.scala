package com.example.akka.actor.kafka

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef}

import scala.collection.concurrent.TrieMap

object TopicManagerActor {

}

class TopicManagerActor(props: Properties) extends Actor with ActorLogging {
  private val topicProducerMap = TrieMap.empty[String, ActorRef]
  private val topicConsumerMap = TrieMap.empty[String, ActorRef]

  override def receive: Receive = ???
}
