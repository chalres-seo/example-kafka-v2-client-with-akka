package com.example.akka.actor.kafka

import java.util.concurrent.Future
import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorLogging, Props}
import com.example.kafka.producer.ProducerClient
import com.example.utils.AppConfig
import org.apache.kafka.common.serialization.Serializer
import com.example.akka.actor.kafka.ProducerActor._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.annotation.tailrec

object ProducerActor {
  private val maxRetryCount = 3

  def props: Props = akka.actor.Props(new ProducerActor(AppConfig.getKafkaProducerProps))
  def props(props:Properties): Props = akka.actor.Props(new ProducerActor(props))

  case class RequestProduceRecord[K, V](requestId: Long, record: ProducerRecord[K, V])
  case class ResponseProduceRecord(requestId: Long, recordMetadata: Option[RecordMetadata])

  case class RequestProduceRecords[K, V](requestId: Long, records: Vector[ProducerRecord[K, V]])
  case class ResponseProduceRecords[K, V](requestId: Long, remainedRecords: Vector[ProducerRecord[K, V]], recordsMetadata: Vector[RecordMetadata])

  case class FailedProduceRecord[K, V](requestId: Long, remainedRecord: ProducerRecord[K, V])
  case class FailedProduceRecords[K, V](requestId: Long, remainedRecords: Vector[ProducerRecord[K, V]])
}

class ProducerActor(props: Properties) extends Actor with ActorLogging {
  type ProduceRecordResultList = Vector[(ProducerRecord[Any, Any], Option[RecordMetadata])]
  
  private var producerClient: ProducerClient[Any, Any] = _

  override def preStart(): Unit = {
    log.info("kafka producer client start")
    producerClient = ProducerClient(props)
  }

  override def postStop(): Unit = {
    log.info("kafka producer client stop")
    producerClient.close()
  }

  override def receive: Receive = {
    case RequestProduceRecord(requestId, record) =>
      log.debug("request produce record.")
      this.produceRecordLoop(maxRetryCount, record) match {
        case Some(recordMetadata) =>
          log.debug(s"succeed produce record, response produce record metadata. " +
            s"metadata: ${ProducerClient.produceRecordMetadataToMap(recordMetadata).mkString(", ")}")

          sender ! ResponseProduceRecord(requestId, Option(recordMetadata))
        case None =>
          log.debug("failed produce record. response empty metadata and logging failed record.")

          sender() ! ResponseProduceRecord(requestId, None)
          context.self ! FailedProduceRecord(requestId, record)
      }

    case RequestProduceRecords(requestId, records) =>
      val produceRecordResultList = this.produceRecordsLoop(maxRetryCount, records, Vector.empty[RecordMetadata])
      val failedProduceRecords = produceRecordResultList._1
      val succeedProduceRecordsMetadata = produceRecordResultList._2

      if (failedProduceRecords.isEmpty) {
        log.debug(s"succeed all produce records, response produce records metadata. " +
          s"succeed produce record count: ${succeedProduceRecordsMetadata.length}, " +
          s"failed produce record count: ${failedProduceRecords.length}, " +
          s"metadata:\n\t" +
          s"${succeedProduceRecordsMetadata.map(ProducerClient.produceRecordMetadataToMap(_).mkString(", ")).mkString("\n\t")}")

        sender() ! ResponseProduceRecords(requestId, failedProduceRecords, succeedProduceRecordsMetadata)
      } else {
        log.debug("failed some produce records, response failed produce records and succeed produce records metadata. " +
          s"succeed produce record count: ${succeedProduceRecordsMetadata.length}, " +
          s"failed produce record count: ${failedProduceRecords.length}, " +
          s"metadata:\n\t" +
          s"${succeedProduceRecordsMetadata.map(ProducerClient.produceRecordMetadataToMap(_).mkString(", ")).mkString("\n\t")}")

        sender() ! ResponseProduceRecords(requestId, failedProduceRecords, succeedProduceRecordsMetadata)
        context.self ! FailedProduceRecords(requestId, failedProduceRecords)
      }

    case FailedProduceRecord(_, remainedRecord) =>
      log.error(s"logging failed produce record. retried count: $maxRetryCount, record: $remainedRecord")

    case FailedProduceRecords(_, remainedRecords) =>
      log.error(s"logging failed produce records. retried count: $maxRetryCount, records:\n\t${remainedRecords.mkString("\n\t")}")
  }

  @tailrec
  private def produceRecordLoop(retryCount: Int, record: ProducerRecord[Any, Any]): Option[RecordMetadata] = {
    if (retryCount > 0) {
      try {
        Option(producerClient.produceRecord(record).get)
      } catch {
        case e: Exception =>
          log.error(s"retry failed produce record. retry count: $retryCount/$maxRetryCount, record: $record", e)
          produceRecordLoop(retryCount - 1, record)
        case t: Throwable =>
          log.error(s"failed produce records unknown exception. record: $record", t)
          None
      }
    } else None
  }

  @tailrec
  private def produceRecordsLoop(retryCount: Int,
                                 produceRecord: Vector[ProducerRecord[Any, Any]],
                                 produceRecordsMetadata: Vector[RecordMetadata]): (Vector[ProducerRecord[Any, Any]], Vector[RecordMetadata]) = {
    if (retryCount > 0) {
      val produceRecordResultList = this.produceRecords(produceRecord)
      val failedProduceRecord = this.getFailedProduceRecords(produceRecordResultList)
      val succeedProduceRecordsMetadata = this.getSucceedProduceRecordsMetadata(produceRecordResultList)

      if (failedProduceRecord.isEmpty) {
        (failedProduceRecord, produceRecordsMetadata ++ succeedProduceRecordsMetadata)
      } else {
        log.error(s"retry failed produce records. " +
          s"retry count: $retryCount/$maxRetryCount, " +
          s"failed record count: ${failedProduceRecord.length}")
        this.produceRecordsLoop(retryCount - 1, failedProduceRecord, produceRecordsMetadata ++ succeedProduceRecordsMetadata)
      }
    } else {
      (produceRecord, produceRecordsMetadata)
    }
  }

  private def produceRecords(records: Vector[ProducerRecord[Any, Any]]): ProduceRecordResultList = {
    records.zip(producerClient.produceRecords(records).map { r =>
      try { Option(r.get) } catch {
        case e: Throwable =>
          log.error(s"failed produce record. ", e)
          None
      }
    })
  }

  private def getFailedProduceRecords(produceRecordResultList: ProduceRecordResultList): Vector[ProducerRecord[Any, Any]] = {
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
