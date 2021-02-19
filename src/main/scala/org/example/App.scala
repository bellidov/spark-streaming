package org.example

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.cli.{BasicParser, CommandLine, Options}
import org.apache.spark.sql.SparkSession

object App extends LazyLogging {

  val ExpediaHDFSPath = "expediaPath"
  val WarehouseDir = "warehouseDir"
  val KafkaBootstrap = "kafkaServer"
  val TopicName = "topicName"
  val OutputPath = "outputPath"

  /**
   * Main method to start the batch processing, it takes arguments from command line, parses them and
   * run the batch reader class inside a Zipkin tracer
   * */
  def main(args: Array[String]): Unit = {
    logger.info("Starting Streaming reading")
    val cmd = getCommandLine(args)

    val sparkSession = SparkSession.builder()
      .appName("example")
      .master("yarn")
      .config("spark.sql.warehouse.dir", WarehouseDir)
      .enableHiveSupport()
      .getOrCreate()

    val kafkaReader = new KafkaReader(cmd.getOptionValue(KafkaBootstrap),
      cmd.getOptionValue(TopicName),
      sparkSession)

    CustomTracer.trace(new StreamReader(sparkSession, kafkaReader,
      cmd.getOptionValue(ExpediaHDFSPath),
      cmd.getOptionValue(OutputPath)))
  }

  /**
   * Gets the incoming args and parse them
   * */
  private def getCommandLine(args: Array[String]): CommandLine = {
    lazy val parser = new BasicParser
    lazy val options = new Options()
      .addOption(
        ExpediaHDFSPath.head.toString,
        ExpediaHDFSPath,
        true,
        "Path to expedia data in HDFS"
      )
      .addOption(
        WarehouseDir.head.toString,
        WarehouseDir,
        true,
        "Warehouse directory"
      )
      .addOption(
        KafkaBootstrap.head.toString,
        KafkaBootstrap,
        true,
        "Kafka server to get the data from topic"
      )
      .addOption(
        TopicName.head.toString,
        TopicName,
        true,
        "Name of the incoming topic from Kafka"
      )
      .addOption(
        OutputPath.head.toString,
        OutputPath,
        true,
        "Output path"
      )

    parser.parse(options, args)
  }
}
