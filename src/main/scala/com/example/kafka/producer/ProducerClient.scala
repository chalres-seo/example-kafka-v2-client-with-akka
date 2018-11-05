package com.example.kafka.producer

import java.util.Properties
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

import com.example.kafka.metric.KafkaMetrics
import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._

import scala.collection.JavaConversions._

class ProducerClient[K, V](kafkaProducer: Producer[K, V], props: Properties) extends LazyLogging {
  private val defaultProduceRecordCallback = ProducerClient.createDefaultProduceRecordCallBack
  private val producerMetrics = KafkaMetrics(kafkaProducer.metrics())

  @throws[Exception]("failed produce record")
  def produceRecord(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    logger.debug(s"produce record to kafka. record: $record")

    kafkaProducer.send(record, defaultProduceRecordCallback)
  }

  @throws[Exception]("failed produce record")
  def produceRecordWithCallback(record: ProducerRecord[K, V])
                    (fn: (RecordMetadata, Exception) => Unit): Future[RecordMetadata] = {
    logger.debug(s"produce single record to kafka. record: $record")

    kafkaProducer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        fn(metadata, exception)
      }
    })
  }

  @throws[Exception]("failed produce records")
  def produceRecords(records: Vector[ProducerRecord[K, V]]): Vector[Future[RecordMetadata]] = {
    logger.debug(s"produce records to kafka. record count: ${records.length}")
    records.map(this.produceRecord)
  }

  @throws[Exception]("failed produce records")
  def produceRecordsWithCallback(records: Vector[ProducerRecord[K, V]])
                     (fn: (RecordMetadata, Exception) => Unit): Vector[Future[RecordMetadata]] = {
    records.map(this.produceRecordWithCallback(_)(fn))
  }

  def getProducerMetrics: KafkaMetrics = this.producerMetrics
  def getProducerProperties: Properties = this.props

  def close(): Unit = {
    logger.info("close kafka producer client.")
    kafkaProducer.close()
  }
}

object ProducerClient extends LazyLogging {
  private val defaultKafkaProducerClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.producer)
  private val kafkaProducerClientIdNum = new AtomicInteger(1)

  def apply[K, V](kafkaProducer: Producer[K, V], props: Properties): ProducerClient[K, V] = {
    new ProducerClient(kafkaProducer, props)
  }

  def apply[K, V](props: Properties): ProducerClient[Any, Any] = {
    this.apply(this.createKafkaProducerClient(props), props)
  }

  def produceRecordMetadataToMap(recordMetadata: RecordMetadata): Map[String, Any] = {
    Map(
      "offset" -> recordMetadata.offset(),
      "partition" -> recordMetadata.partition(),
      "timestamp" -> recordMetadata.timestamp(),
      "topic" -> recordMetadata.topic()
    )
  }

  private def createKafkaProducerClient(props: Properties): KafkaProducer[Any, Any] = {
    this.setKafkaProducerClientId(props)

    logger.info("create kafka producer client.")
    logger.info("kafka producer client configs:\n\t" + props.mkString("\n\t"))

    new KafkaProducer[Any, Any](props)
  }

  private def setKafkaProducerClientId(props: Properties): Unit = {
    val kafkaProducerClient = props.getOrDefault("client.id",
      defaultKafkaProducerClientPrefix) + "-" + kafkaProducerClientIdNum.getAndIncrement()

    logger.debug(s"set kafka producer client id. client.id: $kafkaProducerClient")

    props.setProperty("client.id", kafkaProducerClient)
  }

  private def createDefaultProduceRecordCallBack: Callback = {
    logger.debug("create default producer record call back.")

    new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          logger.debug(s"succeed send record. metadata: ${ProducerClient.produceRecordMetadataToMap(metadata)}")
        } else {
          logger.error(s"failed send record. metadata: ${ProducerClient.produceRecordMetadataToMap(metadata).mkString("\n\t")}", exception)
          throw exception
        }
      }
    }
  }

}