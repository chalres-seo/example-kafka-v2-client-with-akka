package com.example.kafka.consumer

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

class ConsumerClient[K, V](kafkaConsumer: Consumer[K, V], props: Properties) extends LazyLogging {
  private val defaultConsumeRecordOffsetCommitCallback = ConsumerClient.createDefaultConsumeRecordOffsetCommitCallBack

  @throws[IllegalArgumentException]
  @throws[IllegalStateException]
  def subscribeTopic(name: String): Unit = {
    logger.info(s"subscribe topic. name: $name")
    kafkaConsumer.subscribe(Collections.singleton(name))
  }

  def unsubscribeAllTopic(): Unit = {
    logger.info("unsubscribe all topics.")
    kafkaConsumer.unsubscribe()
  }

  def getSubscribeTopicList: util.Set[String] = {
    logger.info("get subscribe topic list.")
    kafkaConsumer.subscription()
  }

  @throws[Exception]("failed consume records")
  def consumeRecord: ConsumerRecords[K, V] = {
    logger.debug("consume record.")
    kafkaConsumer.poll(Duration.ofMillis(3000L))
  }

  @throws[Exception]("failed offset commit")
  def offsetCommit(): Unit = {
    logger.info("commit offset.")
    kafkaConsumer.commitSync()
  }

  @throws[Exception]("failed offset commit")
  def offsetCommitAsync(): Unit = {
    logger.info("async commit offset")
    kafkaConsumer.commitAsync(defaultConsumeRecordOffsetCommitCallback)
  }

  @throws[Exception]("failed offset commit")
  def offsetCommitAsync(fn: (util.Map[TopicPartition, OffsetAndMetadata], Exception) => Unit): Unit = {
    logger.info("async commit offset")
    kafkaConsumer.commitAsync(new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        fn(offsets, exception)
        throw exception
      }
    })
  }

  def close(): Unit = {
    logger.info("close kafka consumer client.")

    kafkaConsumer.close()
  }
}

object ConsumerClient extends LazyLogging {
  private val defaultKafkaConsumerClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.consumer)
  private val kafkaConsumerClientIdNum = new AtomicInteger(1)

  def apply[K, V](kafkaConsumer: Consumer[K, V], props: Properties): ConsumerClient[K, V] = {
    new ConsumerClient(kafkaConsumer, props)
  }

  def apply[K, V](kafkaConsumer: Consumer[K, V]): ConsumerClient[K, V] = {
    this.apply(kafkaConsumer, new Properties)
  }

  def apply(props: Properties): ConsumerClient[Any, Any] = {
    this.apply(this.createKafkaConsumerClient(props), props)
  }

  private def createKafkaConsumerClient(props: Properties): KafkaConsumer[Any, Any] = {
    this.setKafkaConsumerClientId(props)

    logger.info("create kafka consumer client.")
    logger.info("kafka consumer client config:\n\t" + props.mkString("\n\t"))

    new KafkaConsumer[Any, Any](props)
  }

  private def setKafkaConsumerClientId(props: Properties): Unit = {
    props.setProperty("client.id",
      props.getOrDefault("client.id", defaultKafkaConsumerClientPrefix)
        + "-" + kafkaConsumerClientIdNum.getAndIncrement())
  }

  def createDefaultConsumeRecordOffsetCommitCallBack: OffsetCommitCallback = new OffsetCommitCallback {
    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      if (exception == null) {
        logger.debug(s"async commit success, offsets: $offsets")
      } else {
        logger.error(s"commit failed for offsets: $offsets", exception)
        throw exception
      }
    }
  }
}