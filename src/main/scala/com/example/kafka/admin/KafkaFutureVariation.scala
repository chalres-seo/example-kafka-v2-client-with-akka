package com.example.kafka.admin

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaFuture

object KafkaFutureVariation extends LazyLogging {
  def thenApply[ResultT, ReturnT](future: KafkaFuture[ResultT])(fn: ResultT => ReturnT): KafkaFuture[ReturnT] = {
    logger.info("compose kafka then apply.")
    future.thenApply(this.createKafkaFutureBaseFuntion(fn))
  }

  def whenComplete[ResultT](future: KafkaFuture[ResultT])(fn: (ResultT, Throwable) => Unit): KafkaFuture[ResultT] = {
    logger.info("compose kafka when complete.")
    future.whenComplete(this.createKafkaFutureBiConsumer(fn))
  }

  private def createKafkaFutureBaseFuntion[ResultT, ReturnT](fn: ResultT => ReturnT): KafkaFuture.BaseFunction[ResultT, ReturnT] = {
    logger.info("create kafka future base function.")
    new KafkaFuture.BaseFunction[ResultT, ReturnT] {
      override def apply(result: ResultT): ReturnT = fn(result)
    }
  }

  private def createKafkaFutureBiConsumer[ResultT](fn: (ResultT, Throwable) => Unit): KafkaFuture.BiConsumer[ResultT, Throwable] = {
    logger.info("create kafka future bi consumer.")
    new KafkaFuture.BiConsumer[ResultT, Throwable] {
      override def accept(result: ResultT, exception: Throwable): Unit = fn(result, exception)
    }
  }
}
