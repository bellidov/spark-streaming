package org.example

import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll}

class StreamReaderIntegrationTest extends AnyFunSpec with BeforeAndAfterAll with MockFactory with Matchers {

  val ResourcesDirectory = normalizePath(new File("src/test/resources"));
  val BasePathWithProtocol = s"file:///${normalizePath(new File("/tmp/testdata"))}"
  val warehousePath = s"${BasePathWithProtocol}/streaming/warehouse/${System.currentTimeMillis}"
  val outputPath = s"${BasePathWithProtocol}/streaming/out/${System.currentTimeMillis}"
  val expediaPath = s"${ResourcesDirectory}/expedia"

  val sparkSession = SparkSession.builder()
    .appName("example")
    .master("local")
    .config("spark.sql.warehouse.dir", warehousePath)
    .config("spark.sql.crossJoin.enabled", true)
    .enableHiveSupport()
    .getOrCreate()

  val kafkaReader = stub[KafkaReader]

  private var streamReader: StreamReader = _

  override def beforeAll() {
    streamReader = new StreamReader(sparkSession, kafkaReader, expediaPath, outputPath)
  }

  it("Tests the stream processing workflow: expected hotel_id") {
    (kafkaReader.read _).when(0, 1000).returns(getMockedKafkaJson())
    streamReader.run()

    val actualHotelId = sparkSession.sql("select hotel_id from preferences")
      .collectAsList().get(0).toString()

    actualHotelId must be ("[377957122049]")
  }

  it("Tests the stream processing workflow: expected preference") {
    (kafkaReader.read _).when(0, 1000).returns(getMockedKafkaJson())
    streamReader.run()

    val actualPreference = sparkSession.sql("select preference from preferences")
      .collectAsList().get(0).toString()

    actualPreference must be ("[Standart stay]")
  }

  private def normalizePath(f: File) = f.getAbsolutePath.replace('\\', '/')

  private def getMockedKafkaJson(): DataFrame = {
    sparkSession.sql("select '{\"id\":\"377957122049\"," +
      "\"name\":\"test hotel\"," +
      "\"country\":\"test country\"," +
      "\"city\":\"test city\"," +
      "\"address\":\"test address\"," +
      "\"avg_tmpr_c\":\"22\"," +
      "\"wthr_date\":\"2016-10-31\"}' as value")
  }
}
