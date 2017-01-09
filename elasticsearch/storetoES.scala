package com.elasticsearch

//import exam.sales5.Employee2
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark


/**
  * Created by satyak on 9/19/2016.
  */
object storetoES {

  case class Trip(departure: String, arrival: String)

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val spark = SparkSession.builder
      .master("local")
      .appName("ElasticSearch")
      .config("es.index.auto.create", "true")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      // .config("es.nodes","master")
      // .config("es.port",9200)
      .getOrCreate()
    val sc = spark.sparkContext
    //val salesemp = sc.textFile("/tmp/device/")
    import spark.implicits._
    //val DF1 = salesemp.map(_.split(",")).map( row => Employee2(row(0), row(1), row(2).toDouble)).toDF()
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")
    val lastWeekTrip1 = Trip("MUC1", "OTP1")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip,lastWeekTrip1))
    //DF1.rdd.saveToEs("spark/sales")
    // EsSpark.saveToEs(DF1.rdd, "spark/sales")
    EsSpark.saveToEs(rdd, "ambari/sales")
  }
}
