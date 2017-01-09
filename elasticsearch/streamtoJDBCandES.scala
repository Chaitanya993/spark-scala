
//* Created by chait on 9/29/2016.
package com.elasticsearch

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object streamtoJDBCandES {

  def main(args: Array[String]) {

    val brokers = "master:9092,master:9093" //args[0];
    val topics = "heartbeat" //args[1];
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Create context with 2 second batch interval
    val spark = SparkSession.builder
      .master("local")
      .appName("Tweet")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://master:3306/customer"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //read data from kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

      messages.persist()

    // convert input data as stream of HeartBeat objects
    val lines = messages.map(_._2)
    val alllines = lines.flatMap { line => line.split("\n") }
    alllines.foreachRDD(
      m => {
        if (m.count() > 0) {
          val df2 = gettable(m)
          writeTOJDBC(df2, url, prop)
          writetoES(df2)
        }
      }
    )
    // Start the computation
    ssc.start()
    ssc.awaitTermination()

    def gettable(a: RDD[String]): DataFrame = {
      val df = spark.read.json(a)
      df.printSchema()
      df.registerTempTable("tweet")
      val df2 = spark.sql("select text from tweet")
      df2
    }
    def writetoES(df1: DataFrame) = {
      import org.elasticsearch.spark._
      df1.show()
      df1.rdd.saveToEs("newtweet/testingdata")
    }
    def writeTOJDBC(df: DataFrame, url: String, prop: java.util.Properties) = {
      df.write.format("json").mode("append").jdbc(url, "tweet", prop)
    }
  }


}
