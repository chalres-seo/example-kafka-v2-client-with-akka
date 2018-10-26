package com.example.apps

import akka.actor.{ActorRef, ActorSystem, Props}
import com.example.akka.actor.kafka.AdminActor
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.KafkaProducer

import scala.io.StdIn

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("app main start.")

//    val system = ActorSystem.create("example-kafka")
//    val kafkaProducerRef1 = system.actorOf(Props[KafkaProducer[String, String]], "kafka-producer")
//
//
//    println(s"kafka producer actor: $kafkaProducerRef1")
//
//
//    kafkaProducerRef1 ! "printit"
//    kafkaProducerRef1 ! "worker-fail"
//    kafkaProducerRef1 ! "worker-fail"
//    kafkaProducerRef1 ! "print"
//
//    Thread.sleep(1000)
//    system.terminate()
//    waitConsoleForDebug(system)
  }

  def waitConsoleForDebug(system: ActorSystem) = {
    Thread.sleep(5000)
    println(">>> Press ENTER to exit <<<")
    try StdIn.readLine()
    finally {
      logger.debug("system terminate.")
      system.terminate()
    }
  }
}