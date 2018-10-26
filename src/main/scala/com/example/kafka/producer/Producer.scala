package com.example.kafka.producer

import java.util.Properties
import java.util.concurrent.{CompletableFuture, Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.function.Supplier

import com.example.kafka.metric.KafkaMetrics
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

case class Producer[K, V](kafkaProducer: KafkaProducer[K, V], props: Properties) extends LazyLogging {
  private val produceRecordCallback = Producer.createProduceRecordCallBack

  private val kafkaProducerMetrics = kafkaProducer.metrics()
  private val producerMetrics = KafkaMetrics(kafkaProducerMetrics)

  def produceRecord(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    logger.debug(s"write record to kafka by async. record: $record")

    kafkaProducer.send(record, produceRecordCallback)
  }

  def producerRecordWithCallbackFN(record: ProducerRecord[K, V])(fn: (RecordMetadata, Exception) => Unit): Unit = {
    logger.debug(s"write record to kafka by async. record: $record")

    kafkaProducer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = fn(metadata, exception)
    })
  }

  def produceRecords(records: Vector[ProducerRecord[K, V]]): Vector[Future[RecordMetadata]] = {
    records.map(this.produceRecord)
  }

  def getProducerMetrics: KafkaMetrics = this.producerMetrics

  def close(): Unit = {
    logger.info("close kafka producer client.")
    kafkaProducer.close()
  }
}

object Producer extends LazyLogging {
  private val defaultKafkaProducerProps = AppConfig.getKafkaProducerProps

  private val kafkaProducerClientPrefix = AppConfig.getString("kafka.prefix.producer.client.id") + "-"
  private val kafkaProducerClientId = new AtomicInteger(1)

//  private val kafkaProducerCallbackFN = {
//    (metadata: RecordMetadata, exception: Exception) => {
//      if (exception == null) {
//        logger.debug(s"succeed send record. metadata:\n\t" +
//          s"${Producer.recordMetadataToMap(metadata).mkString("\t\n")}")
//      } else {
//        logger.error(s"failed send record. metadata:\n\t" +
//          s"${Producer.recordMetadataToMap(metadata).mkString("\t\n")}", exception)
//      }
//    }
//  }

  private def createKafkaProducerClient[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) = {
    props.setProperty("client.id", this.getKafkaProducerClientId)

    logger.info("create kafka producer client.")
    logger.info("kafka producer client configs:\n\t" + props.mkString("\n\t"))

    new KafkaProducer(props, keySerializer, valueSerializer)
  }

  private def getKafkaProducerClientId: String = kafkaProducerClientPrefix + kafkaProducerClientId.getAndIncrement()

  def apply[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): Producer[K, V] = {
    new Producer(this.createKafkaProducerClient[K, V](defaultKafkaProducerProps, keySerializer, valueSerializer), defaultKafkaProducerProps)
  }

  def apply[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Producer[K, V] = {
    new Producer(this.createKafkaProducerClient[K, V](props, keySerializer, valueSerializer), props)
  }

  def getMockProducer[K, V](mockProducer: KafkaProducer[K, V]): Producer[K, V] = {
    new Producer(mockProducer, defaultKafkaProducerProps)
  }

  private[kafka] def recordMetadataToMap(recordMetadata: RecordMetadata): Map[String, Any] = {
    Map(
      "offset" -> recordMetadata.offset(),
      "partition" -> recordMetadata.partition(),
      "timestamp" -> recordMetadata.timestamp(),
      "topic" -> recordMetadata.topic()
    )
  }

  private[Producer] def createProduceRecordCallBack: Callback = new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        logger.debug(s"succeed send record. metadata: ${Producer.recordMetadataToMap(metadata)}")
      } else {
        logger.error(s"failed send record. metadata: ${Producer.recordMetadataToMap(metadata).mkString("\n\t")}", exception)
      }
    }
  }
}