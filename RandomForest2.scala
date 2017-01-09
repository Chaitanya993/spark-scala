package com.KM

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession


/**
  * Created by satyak on 8/17/2016.
  */
object RandomForest2 {

  val logger = Logger.getLogger(getClass.getName)
  def main(args:Array[String])    {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.sql.warehouse.dir","file:///C:/Experiment/spark-2.0.0-bin-without-hadoop/spark-warehouse")
      //.config("spark.some.config.option", "config-value")
      .getOrCreate()

    val sc = spark.sparkContext
    val df1 = spark.read.json("data/cancer.json")

    //df1.printSchema()
    val df2 = df1.filter("bareNuclei is not null")
    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    val assembler = new VectorAssembler().setInputCols(Array("bareNuclei",
      "blandChromatin", "clumpThickness", "marginalAdhesion", "mitoses",
      "normalNucleoli", "singleEpithelialCellSize", "uniformityOfCellShape",
      "uniformityOfCellSize")).setOutputCol("features")
    val labelIndexer = new StringIndexer().setInputCol("class").setOutputCol("label")
    val test = (a : Int) => a + 1
    val classifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setNumTrees(20)
      .setFeatureSubsetStrategy("auto")
      .setSeed(5043)
    val pipeline    = new Pipeline().setStages(Array(labelIndexer, assembler, classifier))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    predictions.select("sampleCodeNumber", "label", "prediction").show(5)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      //.setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    print(s"accuracy===========================>> $accuracy")

  }
}
