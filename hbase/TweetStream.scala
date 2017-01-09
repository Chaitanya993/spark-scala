package com.hbase

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

import com.datastax.spark.connector.PairRDDFunctions
import com.google.gson.GsonBuilder
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object TweetStream {

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
        .config("spark.cleaner.ttl", "3600")
      .getOrCreate()

    val sc =spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val driver = "com.mysql.jdbc.Driver"
    val url="jdbc:mysql://master:3306/customer"
    val prop = new java.util.Properties
    prop.setProperty("user","root")
    prop.setProperty("password","root")

    //read data from kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)



    // convert input data as stream of HeartBeat objects
    val lines = messages.map(_._2)
    val alllines = lines.flatMap { line => line.split("\n") }
    alllines.foreachRDD(
      m => {
        import spark.implicits._
        //write to Hbase
        val conf = HBaseConfiguration.create()
        val job = Job.getInstance(conf)
        val jobConf = job.getConfiguration
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "tweet")
        jobConf.set(TableInputFormat.INPUT_TABLE, "tweet");
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        if(m.count() > 0) {
          val df =spark.read.json(m)
          df.printSchema()

          df.registerTempTable("tweet")
          val df2= spark.sql("select text, favorited, " +
            "possibly_sensitive,timestamp_ms, " +
            "id , geo ," +
            " retweet_count ,source from tweet")
          writeTOJDBC(df2,url,prop)
          m.map(getTweet).map(writeTOHbase).saveAsNewAPIHadoopDataset(jobConf)

        }
      }
    )
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def getTweet(jsonstr:String):Tweet={
    print(jsonstr)
    val gson = new GsonBuilder().create()

    val hb = gson.fromJson(jsonstr, classOf[Tweet])
    //assign JSON string
    hb.tweetStr=jsonstr
     hb
  }

  //
  def writeTOJDBC(df:DataFrame,url:String,prop:java.util.Properties)={
    df.write.format("json").mode("append").jdbc(url, "tweet", prop)
  }

  //
  def writeTOHbase(data:Tweet) :(ImmutableBytesWritable, Put) = {
    val rowkey = data.id.toString()
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("Tw"), Bytes.toBytes(data.text.toString()));
    put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("retweet_count"), Bytes.toBytes(data.timestamp_ms.toString()));
   put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("tweet"), Bytes.toBytes(data.tweetStr.toString()));

    (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

}