package com.example.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Properties

import com.example.utils.AppConfig.KafkaClientType.KafkaClientType
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

/**
  * Application Config.
  */
object AppConfig extends LazyLogging {
  private val confPath = "conf/application.conf"
  private val conf: Config = this.readConfigFile(confPath)

  private lazy val kafkaAdminProps = this.readPropertiesFile(conf.getString("kafka.admin.props.file"))
  private lazy val kafkaProducerProps = this.readPropertiesFile(conf.getString("kafka.producer.props.file"))
  private lazy val kafkaConsumerProps = this.readPropertiesFile(conf.getString("kafka.consumer.props.file"))

  private def readConfigFile(confFilePath: String): Config = {
    logger.info(s"read config file from: $confFilePath")
    ConfigFactory.parseFile(new File(confFilePath)).resolve()
  }

  private def readPropertiesFile(filePath: String): Properties = {
    logger.info(s"read properties file from : $filePath")
    val props: Properties = new Properties()
    props.load(Files.newInputStream(Paths.get(filePath)))
    props
  }

  def getApplicationName: String = this.conf.getString("application.name")

  def getKafkaAdminProps: Properties = this.kafkaAdminProps
  def getKafkaProducerProps: Properties = this.kafkaProducerProps
  def getKafkaConsumerProps: Properties = this.kafkaConsumerProps

  def getKafkaClientPrefix(clientType: KafkaClientType): String = conf.getString(s"kafka.prefix.${clientType.toString}.client.id")

  object KafkaClientType extends Enumeration {
    type KafkaClientType = Value
    val admin, producer, consumer = Value
  }
}