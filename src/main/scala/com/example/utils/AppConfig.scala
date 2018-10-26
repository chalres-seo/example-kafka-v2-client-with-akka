package com.example.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

object AppConfig extends LazyLogging {
  private val conf: Config = this.readApplicationConfig("conf/application.conf")

  private lazy val kafkaAdminProps = this.readPropertiesFile(conf.getString("kafka.admin.props.file"))
  private lazy val kafkaProducerProps = this.readPropertiesFile(conf.getString("kafka.producer.props.file"))
  private lazy val kafkaConsumerProps = this.readPropertiesFile(conf.getString("kafka.consumer.props.file"))

  private def readApplicationConfig(confFilePath: String): Config = {
    logger.debug(s"read application config from: $confFilePath")
    ConfigFactory.parseFile(new File(confFilePath)).resolve()
  }

  private def readPropertiesFile(filePath: String): Properties = {
    logger.debug(s"read properties file from : $filePath")
    val props: Properties = new Properties()
    props.load(Files.newInputStream(Paths.get(filePath)))
    props
  }

  def getApplicationName: String = this.conf.getString("application.name")
  def getKafkaAdminProps: Properties = this.kafkaAdminProps
  def getKafkaProducerProps: Properties = this.kafkaProducerProps
  def getKafkaConsumerProps: Properties = this.kafkaConsumerProps
  def getString(path: String) = this.conf.getString(path)
}