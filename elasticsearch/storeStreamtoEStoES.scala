package com.elasticsearch

//import exam.sales5.Employee2
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
//import stream.Tweet
/**
  * Created by satyak on 9/19/2016.
  */
object storeStreamtoEStoES {
  case class Trip(departure: String, arrival: String)
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    val brokers = "master:9092,master:9093" //args[0];
    val topics = "heartbeat" //args[1];
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val spark = SparkSession.builder
      .master("local")
      .appName("ElasticSearch")
      .config("es.index.auto.create", "true")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .config("spark.cleaner.ttl" , "3600")
      // .config("es.nodes","master")
      // .config("es.port",9200)
      .getOrCreate()
    val sc =spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // convert input data as stream of HeartBeat objects
    val lines = messages.map(_._2)
    val alllines = lines.flatMap (line => line.split("\n") )
    alllines.foreachRDD(
      m => {
        import spark.implicits._
        if(m.count() > 0) {
          val df =spark.read.json(m)
          df.printSchema()
          df.registerTempTable("tweet")
          //val df2= spark.sql("select id, text from tweet")
          val df2= spark.sql("select id, text from tweet")
          import org.elasticsearch.spark.sql._
          df2.saveToEs("tweets/testdata")
        }
      }
    )
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
