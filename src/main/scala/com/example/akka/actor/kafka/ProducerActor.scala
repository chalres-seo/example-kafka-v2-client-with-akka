package com.example.akka.actor.kafka

import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorLogging, Props}
import com.example.kafka.producer.Producer
import com.example.utils.AppConfig
import org.apache.kafka.common.serialization.Serializer
import com.example.akka.actor.kafka.ProducerActor._
import org.apache.kafka.clients.producer.ProducerRecord

object ProducerActor {
  private val maxRetryCount = 3

  def props[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): Props =
    akka.actor.Props(new ProducerActor(AppConfig.getKafkaProducerProps, keySerializer, valueSerializer))
  def props[K, V](props:Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Props =
    akka.actor.Props(new ProducerActor(props, keySerializer, valueSerializer))

  case class RequestProduceRecord[K, V](requestId: Long, record: ProducerRecord[K, V], retryCount: Int = 1)
  case class RequestProduceRecords[K, V](requestId: Long, records: Vector[ProducerRecord[K, V]], retryCount: Int = 1)
  case class FailedProduceRecord[K, V](requestId: Long, retryRecord: ProducerRecord[K, V], retryCount: Int)
}

class ProducerActor[K <: Any, V <: Any](props: Properties,
                          keySerializer: Serializer[K],
                          valueSerializer: Serializer[V]) extends Actor with ActorLogging {

  private var producer:Producer[K, V] = _

  override def preStart(): Unit = {
    log.info("kafka producer client start")
    producer = Producer(props, keySerializer, valueSerializer)
  }
  override def postStop(): Unit = {
    log.info("kafka producer client stop")
    producer.close()
  }

  override def receive: Receive = {
    case RequestProduceRecord(requestId, record, retryCount) =>
      if (retryCount <= maxRetryCount) {
        try {
          producer.produceRecord(record.asInstanceOf[ProducerRecord[K, V]]).get()
        } catch {
          case e:Exception =>
            log.error(s"failed produce record. record: $record", e)
            log.error(s"retry failed produce record. retry count: $retryCount")
            context.self.tell(RequestProduceRecord(requestId, record, retryCount + 1), sender())
          case t: Throwable =>
            log.error("failed produce record. record: $record", t)
            log.error("unknown exception, skip retry failed produce record.")
            context.self.tell(FailedProduceRecord(requestId, record, retryCount), sender())
        }
      } else {
        log.error(s"exceed retry count $retryCount/$maxRetryCount. stop retry failed produce record. record: $record")
        context.self.tell(FailedProduceRecord(requestId, record, retryCount), sender())
      }

    case FailedProduceRecord(_, retriedRecord, retriedCount) =>
      log.error(s"logging failed produce record. record: $retriedRecord, retry count: $retriedCount")
  }
}
