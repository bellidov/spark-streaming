package org.example

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

class StreamReader(val sparkSession: SparkSession,
                   val kafkaReader: KafkaReader,
                   val ExpediaHDFSPath: String,
                   val OutputPath: String) extends LazyLogging with Runnable{



  val StartOffset = sparkSession.sparkContext.getConf.get("spark.batch.startOffset", "0").toInt
  val BatchLimit = StartOffset.toInt +
    sparkSession.sparkContext.getConf.get("spark.batch.limit", "1000").toInt

  /**
   * Run method of streaming reader, it's used for the main processing of data, starting from the incoming hdfs expedia
   * and kafka sources. In the output, it stores the preferences result in hdfs parquet format.
   * */
  override def run(): Unit = {
    logger.info("Runnable stream started")
    val df1 = readFromHDFSasDataFrame()
    val df2 = readFromHDFSasStream()

    logger.info("starting streaming task:")

    df1.createOrReplaceTempView("incoming_expedia_2016")

    val query = df2
      .writeStream
      .format("memory")
      .queryName("incoming_expedia_2017")
      .start()

    while(!query.awaitTermination(5000)) {
      if(!query.status.isDataAvailable && query.status.isTriggerActive) {
        query.stop()
      }
    }

    logger.info("expedia for 2016: ")
    sparkSession.sql("select hotel_id from incoming_expedia_2016 group by hotel_id").show()
    logger.info("expedia for 2017: ")
    sparkSession.sql("select hotel_id from incoming_expedia_2017 group by hotel_id").show()

    val kafkaDf = kafkaReader.read(StartOffset, BatchLimit)
    kafkaDf.createOrReplaceTempView("incoming_hotels")
    logger.info("Parse incoming hotels data from Kafka")
    sparkSession.sql(loadResource("/hotels_data.sql"))

    logger.info("Enrich expedia data with average temperature by hotel: ")
    sparkSession.sql(loadResource("/streaming/10_enrich_expedia.sql"))
    sparkSession.sql("select * from enriched_expedia limit 20").show()

    logger.info("Calculate the days of stay for each expedia row: ")
    sparkSession.sql(loadResource("/streaming/20_calculate_stays.sql"))
    sparkSession.sql("select * from stays_expedia limit 20").show()

    logger.info("Calculate type of stay")
    sparkSession.sql(loadResource("/streaming/30_calculate_preferences.sql"))
    sparkSession.sql("select * from preferences_expedia limit 20").show()

    logger.info("Calculate stay preference for each hotel based on the most frequent type of stay: ")
    sparkSession.sql(loadResource("/streaming/40_final_output.sql"))
    sparkSession.sql("select * from preferences limit 10").show()

    logger.info("Store valid expedia data to hdfs")
    sparkSession.sql("select * from preferences")
      .write
      .mode(SaveMode.Append)
      .parquet(OutputPath)
  }

  /**
   * Reads parquet expedia data fromspecific HDFS path
   * */
  def readFromHDFSasDataFrame(): DataFrame = {
    logger.info("Start reading from HDFS: " + ExpediaHDFSPath + "/srch_ci_year=2016")
    sparkSession.read.parquet(ExpediaHDFSPath + "/srch_ci_year=2016")
  }

  /**
   * Reads parquet expedia data froms pecific HDFS path in streaming maner
   * */
  def readFromHDFSasStream(): DataFrame = {
    logger.info("Start reading from HDFS")
    val userSchema = sparkSession.read.parquet(ExpediaHDFSPath + "/srch_ci_year=2017").schema
    sparkSession
      .readStream
      .schema(userSchema)
      .parquet(ExpediaHDFSPath + "/srch_ci_year=2017")
  }

  /**
   * It's used to load sql scrip resources in order to process the input data
   * @param resource: it's the name of the sql script we want to use
   * */
  private def loadResource(resource: String): String = {
    if (this.getClass.getResource(resource) == null) {
      throw new Exception(s"The resource $resource couldn't be found")
    }
    Source.fromInputStream(this.getClass.getResourceAsStream(resource)).mkString
  }
}
