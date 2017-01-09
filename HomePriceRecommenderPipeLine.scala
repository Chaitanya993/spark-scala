package com.liner

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.feature.{StandardScalerModel, StandardScaler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}

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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
import com.github.fommil.netlib.BLAS;

case class Home2(mlsNum: Double, city: String,
                 sqFt: Double, bedrooms: Double,
                 bathrooms: Double,
                 garage: Double, age: Double,
                 acres: Double, price: Double)

object HomePriceRecommenderPipeLine extends Serializable {
  val logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Home")
      .config("spark.sql.warehouse.dir","file:///C:/Experiment/spark-2.0.0-bin-without-hadoop/spark-warehouse")
      //.config("spark.some.config.option", "config-value")
      .getOrCreate()

    //sql context
    //val sc = spark.sparkContext

    val base = "data/housing.json"
    val test = "data/housing_test.json"
    val homeData = spark.read.json(base)
    val hometest = spark.read.json(test)
    import spark.implicits._
    val parsed = homeData.toDF().as[Home2]
    parsed.registerTempTable("dataTBL")

    val parsedtest = hometest.toDF().as[Home2]
    parsedtest.registerTempTable("dataTBLTEST")
    //stats ==========================
    val rdd1= parsed.rdd
    val priceStats = Statistics.colStats(rdd1.map(home => Vectors.dense(home.price)))

    println("Price mean: " + priceStats.mean)
    println("Price max: " + priceStats.max)
    println("Price min: " + priceStats.min)

    // filter out anomalous data
    val filtered = rdd1.filter(home => (home.price > 100000.0
      && home.price < 400000.0 && home.sqFt > 1000.0))
    // see how correlated price and square feet are
    val corr = Statistics.corr(filtered.map(home => home.price),
      filtered.map(home => home.sqFt))
    println("Price and square feet corr: " + corr)

    //build Pipeline
    val lr = new LinearRegression()
    lr.setMaxIter(10)
    lr.setRegParam(0.02)

    val assembler = new VectorAssembler()
      .setInputCols(Array("age","bathrooms","bedrooms","garage","sqFt"))
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler,lr))

    val df2= spark.sql("select mlsNum,age,bathrooms,bedrooms,garage,sqFt, " +
      "price label from dataTBL ").na.drop()

    val df2test= spark.sql("select mlsNum,age,bathrooms,bedrooms,garage,sqFt, " +
      "price label from dataTBLTEST ").na.drop()

    //where price > 100000.0 " +
    //  "and  price < 400000.0 AND sqFt > 1000.0").na.drop()

    val model = pipeline.fit(df2)
    val housePredict = model.transform(df2)
      .withColumnRenamed("prediction","prediction")
      .withColumnRenamed("label","Price")
      .select("prediction","Price","mlsNum")
    housePredict.show()

    val rm = new RegressionMetrics(
      housePredict.rdd
        .map(x =>( x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])
        )           )
    logger.info("Test Metrics----------------->")
    logger.info("Test Explained Variance:>>>>>>>>>>>>>>>>>>"+rm.explainedVariance)
    logger.info("Test R^2 Coef:>>>>>>>>>>>>>>>>>>>>>>"+rm.r2)
    logger.info("Test MSE:===========>"+rm.meanSquaredError)
    logger.info("Test RMSE:->>>>>>>>>>>>>>>>>"+rm.rootMeanSquaredError)

    val housePredicttest = model.transform(df2test)
      .withColumnRenamed("prediction","prediction")
      .withColumnRenamed("label","Price")
      .select("prediction","Price","mlsNum")
    housePredicttest.show()

  }

  // parse home price data into case class
  def parse(line: String) = {
    val split = line.split('|')
    val mlsNum = split(0).toDouble
    val city = split(1).toString
    val sqFt = split(2).toDouble
    val bedrooms = split(3).toDouble
    val bathrooms = split(4).toDouble
    val garage = split(5).toDouble
    val age = split(6).toDouble
    val acres = split(7).toDouble
    val price = split(8).toDouble
    Home2(mlsNum, city, sqFt, bedrooms, bathrooms, garage, age, acres, price)
  }

}