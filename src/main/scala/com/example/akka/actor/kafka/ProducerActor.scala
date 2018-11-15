package com.example.akka.actor.kafka

import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.example.kafka.producer.ProducerClient
import com.example.utils.AppConfig
import com.example.akka.actor.kafka.ProducerActor._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.annotation.tailrec

object ProducerActor {
  private val maxRetryCount = 3

  def props(props:Properties): Props = akka.actor.Props(new ProducerActor(props))

  case class RequestProduceRecords[K, V](requestId: Long, records: Vector[ProducerRecord[K, V]])
  case class ResponseProduceRecords[K, V](requestId: Long, remainedRecords: Vector[ProducerRecord[K, V]], recordsMetadata: Vector[RecordMetadata])

  case class FailedProduceRecords[K, V](requestId: Long, remainedRecords: Vector[ProducerRecord[K, V]])
}

class ProducerActor[K, V](props: Properties) extends Actor with ActorLogging {
  type ProduceRecordResultList = Vector[(ProducerRecord[K, V], Option[RecordMetadata])]
  
  private var producerClient: ProducerClient[K, V] = _
  private var producerClientId: String = _

  override def preStart(): Unit = {
    log.info("kafka producer actor start")
    producerClient = ProducerClient[K, V](props)
    producerClientId = producerClient.getClientId
    log.info(s"kafka producer actor client id: $producerClientId")
  }

  override def postStop(): Unit = {
    log.info(s"kafka producer actor stop. client id: $producerClientId")
    producerClient.close()
  }

  override def receive: Receive = {
    case msg: RequestProduceRecords[K, V] =>
      val requestId = msg.requestId
      val records = msg.records
      log.info(s"request produce records.")

      val produceRecordResultList = this.produceRecordsLoop(maxRetryCount, records, Vector.empty[RecordMetadata])
      val failedProduceRecords = produceRecordResultList._1
      val succeedProduceRecordsMetadata = produceRecordResultList._2

      if (failedProduceRecords.isEmpty) {
        log.info(s"request produce result." +
          s" succeed/failed/total: ${succeedProduceRecordsMetadata.length}/${failedProduceRecords.length}/${records.length}")

        sender() ! ResponseProduceRecords(requestId, failedProduceRecords, succeedProduceRecordsMetadata)
      } else {
        log.error(s"request produce result." +
          s" succeed/failed/total: ${succeedProduceRecordsMetadata.length}/${failedProduceRecords.length}/${records.length}")

        sender() ! ResponseProduceRecords(requestId, failedProduceRecords, succeedProduceRecordsMetadata)
        context.self ! FailedProduceRecords(requestId, failedProduceRecords)
      }

    case FailedProduceRecords(_, remainedRecords) =>
      log.error(s"logging failed produce records. " +
        s"retried count: $maxRetryCount, records:\n\t${remainedRecords.mkString("\n\t")}")
  }

  @tailrec
  private def produceRecordsLoop(retryCount: Int,
                                 produceRecord: Vector[ProducerRecord[K, V]],
                                 produceRecordsMetadata: Vector[RecordMetadata]): (Vector[ProducerRecord[K, V]], Vector[RecordMetadata]) = {
    if (retryCount > 0) {
      val produceRecordResultList = this.produceRecords(produceRecord)
      val failedProduceRecord = this.getFailedProduceRecords(produceRecordResultList)
      val succeedProduceRecordsMetadata = this.getSucceedProduceRecordsMetadata(produceRecordResultList)

      if (failedProduceRecord.isEmpty) {
        (failedProduceRecord, produceRecordsMetadata ++ succeedProduceRecordsMetadata)
      } else {
        log.error(s"retry failed produce records. " +
          s"retry count: $retryCount/$maxRetryCount, failed record count: ${failedProduceRecord.length}")
        this.produceRecordsLoop(retryCount - 1,
          failedProduceRecord,
          produceRecordsMetadata ++ succeedProduceRecordsMetadata)
      }
    } else {
      (produceRecord, produceRecordsMetadata)
    }
  }

  private def produceRecords(records: Vector[ProducerRecord[K, V]]): ProduceRecordResultList = {
    records.zip(producerClient.produceRecords(records).map { r =>
      try { Option(r.get) } catch {
        case e: Throwable =>
          log.error(s"failed produce record. ", e)
          None
      }
    })
  }

  private def getFailedProduceRecords(produceRecordResultList: ProduceRecordResultList): Vector[ProducerRecord[K, V]] = {
    for {
      r <- produceRecordResultList
      if r._2.isEmpty
    } yield r._1
  }

  private def getSucceedProduceRecordsMetadata(produceRecordsResult: ProduceRecordResultList): Vector[RecordMetadata] = {
    for {
      r <- produceRecordsResult
      if r._2.isDefined
    } yield r._2.get
  }
}
