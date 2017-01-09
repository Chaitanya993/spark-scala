package com.KM

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
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel


object KmeansEmailIncome {
  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)

    val input = sqlContext.createDataFrame(Seq(
      ("a@email.com", 12000,"M"),
      ("b@email.com", 43000,"M"),
      ("c@email.com", 5000,"F"),
      ("5c@email.com", 50000,"F"),
      ("99c@email.com", 588000,"F"),
      ("d@email.com", 60000,"M")
    )).toDF("email", "income","gender")

    input.show()

    //convert M and F to 0.0 and 1.0
    //Converting categorical attribute labels into label indexes
    val indexer = new StringIndexer().setInputCol("gender")
      .setOutputCol("genderIndex")
    //OneHotEncoder transforms a column with a label index into a column of vectored features
    //Converting categorical label indexes into numerical vectors
    val encoder = new OneHotEncoder()
      .setInputCol("genderIndex")
      .setOutputCol("genderVec")

    //Combining all numerical features into a single feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("income","genderVec"))
      .setOutputCol("features")

    //Fitting a K-Means model on the extracted features

    val kmeans = new KMeans().setK(3)
      .setFeaturesCol("features")
      .setPredictionCol("clusterNumber")

    val pipeline = new Pipeline()
      .setStages(Array(indexer, encoder, assembler, kmeans))
    //The model
    //Predicting using K-Means model to get clusters for each data row
    val kMeansPredictionModel = pipeline.fit(input)

    //centeroids
    println("==========Centroids Start=========")
    kMeansPredictionModel.stages(3)
      .asInstanceOf[KMeansModel]
      .clusterCenters
      .foreach { println }

    println("==========Centroids End=========")

    //show cluster number belong to all data
    val predictionResult = kMeansPredictionModel.transform(input)
    predictionResult.show()

    //=========New data. Show Cluster
    val input2 = sqlContext.createDataFrame(Seq(
      ("a@email.com", 20000000,"F")
    )).toDF("email", "income","gender")
    val predictionResult2 = kMeansPredictionModel.transform(input2)
    predictionResult2.show()
  }

}