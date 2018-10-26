package com.example.kafka.admin

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.KafkaFuture

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap


case class Admin(kafkaAdminClient: KafkaAdminClient, props: Properties) extends LazyLogging {
  private val topicNameListCache = TrieMap.empty[String, Unit]
  this.refreshTopicNameListCache()

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

  def getTopicNameList: collection.Set[String] = topicNameListCache.snapshot().keySet

  private def getTopicNameListFromKafka: KafkaFuture[util.Set[String]] = {
    logger.info("get topic name list from kafka.")

    kafkaAdminClient.listTopics().names()
  }

  def refreshTopicNameListCache(): Unit = {
    logger.info("refresh topic name list.")

    topicNameListCache.clear()
    this.getTopicNameListFromKafka.get.foreach(topicNameListCache.update(_, Unit))
  }

  def isExistTopic(name: String): Boolean = {
    logger.info(s"check topic exist at cache. name: $name")

    topicNameListCache.contains(name)
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
          topicNameListCache.update(name, Unit)
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
          topicNameListCache.remove(name)
        } else {
          logger.error(s"failed delete topic. name: $name", exception)
        }
      }
    } else {
      dummyKafkaFuture
    }
  }

//  @throws[ExecutionException]("failed delete all topic.")
//  def deleteAllTopics(): KafkaFuture[Void] = {
//    logger.info("delete all topics.")
//
//    topicNameListCache.clear()
//
//    if (topicNameListCache.isEmpty) {
//      dummyKafkaFuture
//    } else {
//      KafkaFutureVariation.whenComplete {
//        kafkaAdminClient.deleteTopics(this.getTopicNameList).all()
//      } { (_, exception) =>
//       if (exception == null) {
//         logger.info("success delete all topic.")
//       } else {
//         logger.error("failed delete all topic.", exception)
//       }
//      }
//    }
//
//  }

//  def getConsumerGroupIdList: KafkaFuture[util.Collection[ConsumerGroupListing]] = {
//    logger.debug("get consumer group id list.")
//    kafkaAdminClient.listConsumerGroups().all()
//  }
//
//  def getConsumerGroupOffset(groupId: String): KafkaFuture[util.Map[TopicPartition, OffsetAndMetadata]] = {
//    logger.debug(s"get consumer group offsets. group id: $groupId")
//    kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
//  }
//
//  @throws[ExecutionException]("failed get topic partition info.")
//  @throws[UnknownTopicOrPartitionException]("topic or partition is not exist.")
//  def getTopicPartitionInfo(name: String): KafkaFuture[util.List[TopicPartitionInfo]] = {
//    logger.debug(s"get topic partition info. name: $name")
//    KafkaFutureVariation
//      .thenApply(this.describeTopic(name))(_.get(name).partitions())
//  }
//
//  @throws[ExecutionException]("failed get topic partition info.")
//  @throws[UnknownTopicOrPartitionException]("topic or partition is not exist.")
//  def getTopicPartitionCount(name: String): KafkaFuture[Int] = {
//    KafkaFutureVariation
//      .thenApply(this.getTopicPartitionInfo(name))(_.size())
//  }

  def close(): Unit = {
    kafkaAdminClient.close()
  }
}


object Admin extends LazyLogging {
  private val defaultKafkaAdminProps = AppConfig.getKafkaAdminProps

  private val kafkaAdminClientPrefix = AppConfig.getString("kafka.prefix.admin.client.id") + "-"
  private val kafkaAdminClientId = new AtomicInteger(1)

  private def createKafkaAdminClient(props: Properties): KafkaAdminClient = {
    logger.info("create kafka admin client.")
    logger.info("kafka admin client configs:\n\t" + props.mkString("\n\t"))
    AdminClient.create(props).asInstanceOf[KafkaAdminClient]
  }

  private def getKafkaAdminClientId: String = kafkaAdminClientPrefix + kafkaAdminClientId.getAndIncrement()

  def apply(): Admin = {
    defaultKafkaAdminProps.setProperty("client.id", this.getKafkaAdminClientId)
    new Admin(this.createKafkaAdminClient(defaultKafkaAdminProps), defaultKafkaAdminProps)
  }

  def apply(props: Properties): Admin = {
    defaultKafkaAdminProps.setProperty("client.id", this.getKafkaAdminClientId)
    new Admin(this.createKafkaAdminClient(props), props)
  }
}