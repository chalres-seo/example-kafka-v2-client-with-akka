package com.example.kafka.admin

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.KafkaFuture

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class AdminClient(kafkaAdminClient: KafkaAdminClient, props: Properties) extends LazyLogging {
  private val dummyKafkaFuture: KafkaFuture[Void] = KafkaFuture.completedFuture(null)

  @throws[ExecutionException]("failed get describe topic.")
  @throws[UnknownTopicOrPartitionException]("topic is not exist.")
  def describeTopic(name: String): KafkaFuture[util.Map[String, TopicDescription]] = {
    logger.info(s"get describe topic. name: $name")

    kafkaAdminClient
      .describeTopics(util.Collections.singletonList(name))
      .all
  }

  @throws[ExecutionException]("failed get describe topic.")
  @throws[UnknownTopicOrPartitionException]("topic is not exist.")
  def describeTopics(nameList: Vector[String]): util.Map[String, KafkaFuture[TopicDescription]] = {
    logger.info("get describe topics. name list:\n" + nameList.mkString("\n\t"))

    kafkaAdminClient
      .describeTopics(nameList.asJava)
      .values()
  }

  @throws[ExecutionException]("failed get topic list.")
  def getTopicNameList: KafkaFuture[util.Set[String]] = {
    kafkaAdminClient
      .listTopics()
      .names()
  }

  private def getTopicNameListFromKafka: KafkaFuture[util.Set[String]] = {
    logger.info("get topic name list from kafka.")

    kafkaAdminClient.listTopics().names()
  }

  def isExistTopic(name: String): Boolean = {
    logger.info(s"check topic exist. name: $name")

    try {
      this.describeTopic(name).get.get(name).name() == name
    } catch {
      case _:Throwable => false
    }
  }

  @throws[ExecutionException]("failed create topic.")
  def createTopic(name: String, partitionCount: Int, replicationFactor: Short): KafkaFuture[Void] = {
    logger.info(s"create topic. name: $name, partition count: $partitionCount, replication factor: $replicationFactor")

    if (this.isExistTopic(name)) {
      dummyKafkaFuture
    } else {
      KafkaFutureVariation.whenComplete {
        kafkaAdminClient
          .createTopics(util.Collections.singleton(new NewTopic(name, partitionCount, replicationFactor)))
          .all()
      } { (_, exception) =>
        if (exception == null) {
          logger.info(s"succeed create topic. name: $name")
        } else {
          logger.error(s"failed create topic. name: $name", exception)
        }
      }
    }

  }

  @throws[ExecutionException]("failed delete topic.")
  def deleteTopic(name: String): KafkaFuture[Void] = {
    logger.info(s"delete topic. name: $name")

    if (this.isExistTopic(name)) {
      KafkaFutureVariation.whenComplete {
        kafkaAdminClient
          .deleteTopics(util.Collections.singleton(name))
          .all()
      } { (_, exception) =>
        if (exception == null) {
          logger.info(s"succeed delete topic. name: $name")
        } else {
          logger.error(s"failed delete topic. name: $name", exception)
        }
      }
    } else {
      dummyKafkaFuture
    }
  }

  def close(): Unit = {
    kafkaAdminClient.close()
  }
}


object AdminClient extends LazyLogging {
  private val defaultKafkaAdminClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.admin)
  private val kafkaAdminClientIdNum = new AtomicInteger(1)

  private def createKafkaAdminClient(props: Properties): KafkaAdminClient = {
    logger.info("create kafka admin client.")
    logger.info("kafka admin client configs:\n\t" + props.mkString("\n\t"))
    org.apache.kafka.clients.admin.AdminClient.create(props).asInstanceOf[KafkaAdminClient]
  }

  private def setKafkaAdminClientId(props: Properties): Unit = {
    props.setProperty("client.id",
      props.getOrDefault("client.id", defaultKafkaAdminClientPrefix)
        + "-" + kafkaAdminClientIdNum.getAndIncrement())
  }

  def apply(props: Properties): AdminClient = {
    this.setKafkaAdminClientId(props)
    new AdminClient(this.createKafkaAdminClient(props), props)
  }
}