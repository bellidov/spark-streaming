package org.example

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaReader(val bootStrapServer: String,
                  val topicName: String,
                  val sparkSession: SparkSession) extends LazyLogging {

  /**
   * Reads the data from Kafka queue in a batch manner, specifying the start and end offsets
   * */
  def read(startOffset: Int, batchLimit: Int): DataFrame = {
    logger.info(s"Start reading from kafka, start from $startOffset with limit $batchLimit")
    sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", s"""{"$topicName":{"0":$startOffset}}""")
      .option("endingOffsets", s"""{"$topicName":{"0":$batchLimit}}""")
      .option("failOnDataLoss", false)
      .load()
  }

}
