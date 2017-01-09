package com.liner

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
//import org.apache.spark.ml.regression.LinearRegression
//import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
import com.github.fommil.netlib.BLAS;

object BikeLinearRegression {
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.sql.warehouse.dir", "file:///C:/Experiment/spark-2.0.0-bin-without-hadoop/spark-warehouse")
      //.config("spark.some.config.option", "config-value")
      .getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    import spark.implicits._
    //Read Training data
    /*val data = loadTrainingData(spark)

    //build pipeline model
    val ppl = LRPipeline

    //test model with training data
    val model = ppl.fit(data)*/

    val test = spark.read.format("csv").option("header", "true").load("data/sales.txt")

    test.createOrReplaceTempView("temtbl")

    val testsql = spark.sql("select a,d from temtbl").show()
    //   /* val trainingPredict = model.transform(data)
    //      .withColumnRenamed("prediction","Predictcnt")
    //      .withColumnRenamed("label","Inputcnt")
    //      .withColumnRenamed("instant","instant")
    //      //.select("instant", "Predictcnt","Inputcnt")
    //    trainingPredict.show()
    //
    //    //Predict using test date
    //    val dfPredict = loadTestgData(spark)
    //    val predict = model.transform(dfPredict)
    //      .withColumnRenamed("prediction","Predictcnt")
    //      .withColumnRenamed("label","Inputcnt")
    //      .withColumnRenamed("instant","instant")
    //      .select("instant", "Predictcnt","Inputcnt")
    //
    //    println("==================Started ========================")
    //    predict.show()
    //    println("==================End=============================")
    //
    //    val regparams = new RegressionMetrics(predict.map(_.mkString(",")).map(_.split(",")).rdd
    //                                            .map(a => (a(0).toDouble,a(1)toDouble)))
    //    println("================" + regparams.r2 + "===================")
    //  }
    //
    //
    //  def LRPipeline():Pipeline = {
    //    val lr = new LinearRegression()
    //    lr.setMaxIter(1000)
    //    lr.setRegParam(0.01)
    //    val assembler = new VectorAssembler().setInputCols(
    //      Array("weekday","workingday","holiday","weathersit",
    //        "temp","atemp","hum","windspeed"))
    //      .setOutputCol("features")
    //    val pipeline = new Pipeline().setStages(Array(assembler,lr))
    //    return pipeline
    //  }
    //
    //
    //
    //  def loadTrainingData(Sparksession:SparkSession):DataFrame = {
    //    val df = Sparksession.read.json("data/bike.json").na.drop()
    //    df.registerTempTable("bikeDataTbl")
    //    Sparksession.sql("select instant," +
    //      "cast (dteday as date) dteday, " +
    //      "cast (season as int) season, cast(yr as int) yr, " +
    //      "cast(mnth as int) mnth, cast (hr as int) hr, " +
    //      "cast (holiday as int) holiday, cast (weekday as int) weekday, " +
    //      "cast (workingday as int) workingday,    " +
    //      "cast (weathersit as int) weathersit, " +
    //      "cast (temp as double) temp ,cast (atemp as double) atemp, " +
    //      "cast (hum as double) hum, cast (windspeed as int) windspeed,        " +
    //      "cast (casual as int) casual, cast (registered as int) registered, " +
    //      "cast (cnt as Double) label from bikeDataTbl")
    //  }
    //
    //  def loadTestgData(Sparksession:SparkSession):DataFrame = {
    //    val df = Sparksession.read.json("data/bike Test.json").na.drop()
    //    df.registerTempTable("bikeDataTbl")
    //    Sparksession.sql("select instant,cast (dteday as date) dteday, " +
    //      "cast (season as int) season, cast(yr as int) yr, " +
    //      "cast(mnth as int) mnth, cast (hr as int) hr, cast (holiday as int) holiday, " +
    //      "cast (weekday as int) weekday, cast (workingday as int) workingday,    " +
    //      "cast (weathersit as int) weathersit, cast (temp as double) temp ," +
    //      "cast (atemp as double) atemp, cast (hum as double) hum, " +
    //      "cast (windspeed as int) windspeed,        " +
    //      "cast (casual as int) casual, cast (registered as int) registered, " +
    //      "cast (cnt as Double)  label from bikeDataTbl")
    //  }*/


  }
}
